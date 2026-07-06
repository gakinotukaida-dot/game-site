#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
バックテスト採点器 ── as-of再構成 ＋ ①持続/②追随 ＋ lift（読み取り専用・印字のみ・本番非変更）
置き場所: scripts/backtest_hit.py

当たり定義（確定・叩き台 2026-07-06 / HANDOFF §8）:
  ① 持続（主）: 載せた伸びが本物か（崩落せず高止まり）。
  ② 追随（副）: 載せた後さらに伸びたか（早期性）。

やること:
  過去の各 as-of 日 t について、view02 の「検出層(Aサージ)」ランキングを *当時のデータだけ* で復元し
  （now() を t に差し替え・窓/係数は view02_rank_v2.py の本番と一致）、その後 [t, t+H] で ①/② を採点。
  さらに『素のCCU上位(B0)を並べただけ』の当たり率と比べて lift（=芸があるか）を出す。

  無リーク: baseline/recent は ≤t、前向き窓は >t のみ、母集団も ≤t の CCU から（未来を1点も混ぜない）。
  v1の割り切り: きっかけboost・B1(Twitch)は母集団復元に含めない（B1は保存が無く as-of 復元不能／boostは上限0.30の補正で
                順位の背骨はサージ）。=> v1母集団 = サージ順位。

  read-only（SELECTのみ・物理的に書けない固定）。書き込み・ファイル変更・commit は一切しない。
  ※ 先に diagnose_ccu_history.py [1]〜[4] で『データが足りるか・暫定しきい値の当たり率感』を確認してから回すこと。

前提スキーマ（確定済）: player_counts(appid, player_count, recorded_at) / games(appid, name, ...)
"""
import os
import datetime
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]

# --- 母集団（view02 と同一の既定） ---
MIN_CURRENT = int(os.environ.get("MIN_CURRENT") or "100")
TOP_N       = int(os.environ.get("TOP_N")       or "30")   # surfaced 母集団の件数
CAND_N      = int(os.environ.get("CAND_N")      or "45")   # 収縮比で絞る候補数（view02 と同一）
ACTIVE_DAYS = int(os.environ.get("ACTIVE_DAYS") or "3")

# --- 窓・係数（view02_rank_v2.py の本番と一致） ---
BASE_DAYS    = int(os.environ.get("BASE_DAYS")    or "14")
GAP_DAYS     = int(os.environ.get("GAP_DAYS")     or "1")
RECENT_HOURS = int(os.environ.get("RECENT_HOURS") or "6")
RECENT_Q     = float(os.environ.get("RECENT_Q")   or "0.9")
N0           = float(os.environ.get("N0")         or "10")
MIN_POINTS   = int(os.environ.get("MIN_POINTS")   or "5")
Z_REF        = float(os.environ.get("Z_REF")      or "3")
SIG_FLOOR    = float(os.environ.get("SIG_FLOOR")  or "0.3")

# --- 当たりしきい値・グリッド（すべて暫定・diagnose_ccu_history[4] で確定） ---
H_DAYS           = int(os.environ.get("H_DAYS")           or "7")
FORWARD_MIN_DAYS = int(os.environ.get("FORWARD_MIN_DAYS") or "3")
PERSIST_FRAC     = float(os.environ.get("PERSIST_FRAC")   or "0.6")
RISE_MULT        = float(os.environ.get("RISE_MULT")      or "1.3")
GRID_STRIDE_DAYS = int(os.environ.get("GRID_STRIDE_DAYS") or "1")
GRID_MAX         = int(os.environ.get("GRID_MAX")         or "45")  # 採点する as-of 日数の上限（コスト）
EXAMPLES         = int(os.environ.get("EXAMPLES")         or "12")


def _fmt(x, nd=2):
    return "—" if x is None else f"{x:.{nd}f}"


def _pct(a, b):
    return "—" if not b else f"{100.0 * a / b:.0f}%"


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def base_score(shrunk_ratio, z):
    """view02 と同一: magnitude(shrunk) を有意性でソフト減衰。z低でも SIG_FLOOR は残す。"""
    if shrunk_ratio is None:
        return 0.0
    zz = 0.0 if z is None else z
    sig = SIG_FLOOR + (1 - SIG_FLOOR) * clamp(zz / Z_REF, 0, 1)
    return 1 + (shrunk_ratio - 1) * sig


def surge_topn_asof(cur, t, n):
    """as-of t の『検出層(Aサージ)』Top-n を [(appid, R, base_med)] で返す（view02 の now()→t 版）。"""
    cur.execute(
        "WITH latest AS ("
        "  SELECT DISTINCT ON (appid) appid, player_count AS current_ccu "
        "  FROM player_counts WHERE recorded_at <= %(t)s ORDER BY appid, recorded_at DESC), "
        "recent AS ("
        "  SELECT appid, percentile_cont(%(rq)s) WITHIN GROUP (ORDER BY player_count) AS recent_q "
        "  FROM player_counts "
        "  WHERE recorded_at <= %(t)s AND recorded_at > %(t)s - make_interval(hours => %(rh)s) "
        "  GROUP BY appid), "
        "base AS ("
        "  SELECT appid, "
        "    percentile_cont(0.5)  WITHIN GROUP (ORDER BY player_count) AS baseline, "
        "    percentile_cont(0.25) WITHIN GROUP (ORDER BY player_count) AS q1, "
        "    percentile_cont(0.75) WITHIN GROUP (ORDER BY player_count) AS q3, "
        "    count(*) AS n_points "
        "  FROM player_counts "
        "  WHERE recorded_at <  %(t)s - make_interval(days => %(gap)s) "
        "    AND recorded_at >= %(t)s - make_interval(days => %(base)s) "
        "  GROUP BY appid) "
        "SELECT l.appid, l.current_ccu, "
        "  COALESCE(r.recent_q, l.current_ccu) AS recent_value, "
        "  b.baseline, b.q1, b.q3, b.n_points "
        "FROM latest l LEFT JOIN recent r USING (appid) LEFT JOIN base b USING (appid) "
        "WHERE l.current_ccu >= %(min_current)s AND b.n_points >= %(min_points)s "
        "ORDER BY 1 + (b.n_points::float / (b.n_points + %(n0)s)) "
        "              * (COALESCE(r.recent_q, l.current_ccu)::float / NULLIF(b.baseline, 0) - 1) "
        "         DESC NULLS LAST "
        "LIMIT %(cand_n)s",
        {"t": t, "rq": RECENT_Q, "rh": RECENT_HOURS, "gap": GAP_DAYS, "base": BASE_DAYS,
         "min_current": MIN_CURRENT, "min_points": MIN_POINTS, "n0": N0, "cand_n": CAND_N})
    scored = []
    for appid, ccu, recent_value, baseline, q1, q3, npts in cur.fetchall():
        raw = (recent_value / baseline) if baseline else None
        shrunk = None if raw is None else 1 + (npts / (npts + N0)) * (raw - 1)
        z = ((recent_value - baseline) / (q3 - q1)) if (baseline is not None and q3 is not None
                                                        and q1 is not None and (q3 - q1)) else None
        scored.append((appid, recent_value, baseline, base_score(shrunk, z)))
    scored.sort(key=lambda r: r[3], reverse=True)
    return [(a, R, b) for (a, R, b, _s) in scored[:n]]


def ccu_topn_asof(cur, t, n):
    """as-of t の素のCCU上位 n を [(appid, ccu)] で返す（B0 ベースライン）。"""
    cur.execute(
        "WITH latest AS ("
        "  SELECT DISTINCT ON (appid) appid, player_count AS ccu "
        "  FROM player_counts "
        "  WHERE recorded_at <= %s AND recorded_at >= %s - make_interval(days => %s) "
        "  ORDER BY appid, recorded_at DESC) "
        "SELECT appid, ccu FROM latest WHERE ccu >= %s ORDER BY ccu DESC LIMIT %s",
        (t, t, ACTIVE_DAYS, MIN_CURRENT, n))
    return cur.fetchall()


def metrics_asof(cur, ids, t, t1):
    """ids について as-of t の baseline/recent と前向き窓 (t,t1] の中央値/ピーク/点数/観測日数を返す。"""
    out = {}
    if not ids:
        return out
    cur.execute(
        "SELECT appid, "
        " percentile_cont(0.5) WITHIN GROUP (ORDER BY player_count) "
        "   FILTER (WHERE recorded_at <  %(t)s - make_interval(days => %(gap)s) "
        "             AND recorded_at >= %(t)s - make_interval(days => %(base)s)) AS base_med, "
        " count(*) FILTER (WHERE recorded_at <  %(t)s - make_interval(days => %(gap)s) "
        "                   AND recorded_at >= %(t)s - make_interval(days => %(base)s)) AS base_n, "
        " percentile_cont(%(rq)s) WITHIN GROUP (ORDER BY player_count) "
        "   FILTER (WHERE recorded_at <= %(t)s "
        "             AND recorded_at > %(t)s - make_interval(hours => %(rh)s)) AS r_p90, "
        " percentile_cont(0.5) WITHIN GROUP (ORDER BY player_count) "
        "   FILTER (WHERE recorded_at > %(t)s AND recorded_at <= %(t1)s) AS fwd_med, "
        " max(player_count) FILTER (WHERE recorded_at > %(t)s AND recorded_at <= %(t1)s) AS fwd_max, "
        " count(*) FILTER (WHERE recorded_at > %(t)s AND recorded_at <= %(t1)s) AS fwd_n "
        "FROM player_counts "
        "WHERE appid = ANY(%(ids)s) "
        "  AND recorded_at >= %(t)s - make_interval(days => %(base)s) AND recorded_at <= %(t1)s "
        "GROUP BY appid",
        {"t": t, "t1": t1, "gap": GAP_DAYS, "base": BASE_DAYS, "rq": RECENT_Q,
         "rh": RECENT_HOURS, "ids": ids})
    for appid, bmed, bn, rp90, fmed, fmax, fn in cur.fetchall():
        out[appid] = dict(bmed=bmed, bn=bn or 0, rp90=rp90, fmed=fmed, fmax=fmax, fn=fn or 0)
    return out


def forward_daymax(cur, ids, t, t1):
    """前向き窓 (t,t1] の『各日の最大CCU』を {appid: [max,...]} で返す（①の“≥K日 高止まり”判定用）。"""
    out = {}
    if not ids:
        return out
    cur.execute(
        "SELECT appid, date_trunc('day', recorded_at) AS d, max(player_count) "
        "FROM player_counts "
        "WHERE appid = ANY(%s) AND recorded_at > %s AND recorded_at <= %s "
        "GROUP BY appid, d",
        (ids, t, t1))
    for appid, _d, mx in cur.fetchall():
        out.setdefault(appid, []).append(mx)
    return out


def score_one(m, R, daymaxes):
    """1件を ①/② で採点。戻り: (scorable, hit1, hit2) または (False, ...) 採点不能。"""
    if m is None or m["bn"] < MIN_POINTS or m["fn"] == 0 or R is None or R <= 0:
        return (False, False, False)
    thr = max((m["bmed"] or 0) * 1.5, PERSIST_FRAC * R)
    days_above = sum(1 for mx in daymaxes if mx is not None and mx >= thr)
    hit1 = (m["fmed"] is not None and m["fmed"] >= PERSIST_FRAC * R
            and days_above >= FORWARD_MIN_DAYS)
    hit2 = (m["fmax"] is not None and m["fmax"] >= RISE_MULT * R)
    return (True, hit1, hit2)


def main():
    need_days = BASE_DAYS + GAP_DAYS + H_DAYS
    print("=" * 78)
    print("バックテスト採点器（as-of再構成 ＋ ①持続/②追随 ＋ lift・読み取り専用・印字のみ）")
    print(f"当たり: ①持続=窓中央値≥{PERSIST_FRAC}×R かつ 高止まり≥{FORWARD_MIN_DAYS}日 / "
          f"②追随=窓ピーク≥{RISE_MULT}×R（副）  H={H_DAYS}日")
    print(f"母集団: サージTop-{TOP_N}（cand {CAND_N}）  窓: base{BASE_DAYS}/gap{GAP_DAYS}/recent{RECENT_HOURS}h p{RECENT_Q}")
    print(f"グリッド: 直近{GRID_MAX}日ぶんを{GRID_STRIDE_DAYS}日刻み  採点に要る最短履歴={need_days}日")
    print("=" * 78)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT min(recorded_at), max(recorded_at) FROM player_counts")
            mn, mx = cur.fetchone()
            if not (mn and mx):
                print("player_counts が空。中止。")
                return

            # as-of 日グリッド: [mn+need_before, mx-H] を stride 刻み、直近 GRID_MAX 個
            t_last = mx - datetime.timedelta(days=H_DAYS)
            t_first = mn + datetime.timedelta(days=BASE_DAYS + GAP_DAYS)
            grid = []
            t = t_last
            while t >= t_first:
                grid.append(t)
                t = t - datetime.timedelta(days=GRID_STRIDE_DAYS)
            grid.reverse()
            dropped = max(0, len(grid) - GRID_MAX)
            if dropped:
                grid = grid[-GRID_MAX:]  # 直近側を優先採用

            print(f"\n[1] グリッド範囲")
            print(f"  データ期間: {mn} 〜 {mx}")
            if not grid:
                print(f"  採点可能な as-of 日が無い（履歴 < {need_days}日）。[1]の蓄積待ち、または H を短く。")
                return
            print(f"  as-of: {grid[0]} 〜 {grid[-1]}（{len(grid)}日を採点）"
                  + (f"／古い {dropped}日は GRID_MAX={GRID_MAX} 上限で不採用（暗黙間引きしない・明示）" if dropped else ""))

            # 集計器
            agg = dict(s_score=0, s_h1=0, s_h2=0, s_unscored=0,   # surge 母集団
                       b_score=0, b_h1=0, b_h2=0)                  # B0（素CCU上位）
            per_t = []       # (t, s_rate1, s_n)
            last_examples = []

            for gi, tt in enumerate(grid):
                t1 = tt + datetime.timedelta(days=H_DAYS)
                try:
                    P = surge_topn_asof(cur, tt, TOP_N)        # [(appid,R,base_med)]
                    B0 = ccu_topn_asof(cur, tt, TOP_N)         # [(appid,ccu)]
                except Exception as e:
                    print(f"  ⚠ t={tt} 母集団復元 失敗: {type(e).__name__}: {e}")
                    continue
                ids = sorted({a for a, _, _ in P} | {a for a, _ in B0})
                m = metrics_asof(cur, ids, tt, t1)
                dm = forward_daymax(cur, ids, tt, t1)
                ccu0 = {a: c for a, c in B0}

                # surge 母集団の採点
                s_n = s1 = s2 = 0
                exrows = []
                for appid, R, bmed in P:
                    mm = m.get(appid)
                    RR = R if R is not None else ccu0.get(appid)
                    if mm is not None and (RR is None or RR <= 0):
                        RR = mm.get("rp90")
                    scorable, h1, h2 = score_one(mm, RR, dm.get(appid, []))
                    if not scorable:
                        agg["s_unscored"] += 1
                        continue
                    s_n += 1
                    s1 += h1
                    s2 += h2
                    if gi == len(grid) - 1:
                        exrows.append((appid, RR, mm["bmed"], mm["fmed"], mm["fmax"],
                                       len(dm.get(appid, [])), h1, h2))
                agg["s_score"] += s_n
                agg["s_h1"] += s1
                agg["s_h2"] += s2
                per_t.append((tt, (s1 / s_n) if s_n else None, s_n))

                # B0 の採点（同じ窓・同じ採点）
                for appid, ccu in B0:
                    mm = m.get(appid)
                    RR = mm.get("rp90") if (mm and mm.get("rp90") is not None) else ccu
                    scorable, h1, h2 = score_one(mm, RR, dm.get(appid, []))
                    if not scorable:
                        continue
                    agg["b_score"] += 1
                    agg["b_h1"] += h1
                    agg["b_h2"] += h2

                if gi == len(grid) - 1:
                    # 例示は R 降順
                    names = {}
                    if exrows:
                        cur.execute("SELECT appid, name FROM games WHERE appid = ANY(%s)",
                                    ([a for a, *_ in exrows],))
                        names = {a: n for a, n in cur.fetchall()}
                    exrows.sort(key=lambda r: (r[1] or 0), reverse=True)
                    last_examples = [(names.get(a, f"appid:{a}"), RR, b, fm, fx, dd, h1, h2)
                                     for (a, RR, b, fm, fx, dd, h1, h2) in exrows]

            # ---------- [2] surge 母集団 ----------
            s_sc = agg["s_score"]
            print(f"\n[2] サージ母集団（Top-{TOP_N}・全 {len(grid)} as-of 日の合算）")
            print(f"  採点できた件数={s_sc}（採点不能={agg['s_unscored']}）")
            print(f"  ①持続 当たり率 = {agg['s_h1']}/{s_sc}（{_pct(agg['s_h1'], s_sc)}）")
            print(f"  ②追随 当たり率 = {agg['s_h2']}/{s_sc}（{_pct(agg['s_h2'], s_sc)}）副指標")

            # ---------- [3] B0 と lift ----------
            b_sc = agg["b_score"]
            s1r = (agg["s_h1"] / s_sc) if s_sc else None
            s2r = (agg["s_h2"] / s_sc) if s_sc else None
            b1r = (agg["b_h1"] / b_sc) if b_sc else None
            b2r = (agg["b_h2"] / b_sc) if b_sc else None
            lift1 = (s1r / b1r) if (s1r is not None and b1r) else None
            lift2 = (s2r / b2r) if (s2r is not None and b2r) else None
            print(f"\n[3] ベースライン B0（素のCCU上位 Top-{TOP_N}）と lift")
            print(f"  B0 ①当たり率 = {_pct(agg['b_h1'], b_sc)}  B0 ②当たり率 = {_pct(agg['b_h2'], b_sc)}（採点 {b_sc}件）")
            print(f"  → lift① = {_fmt(lift1)}   lift② = {_fmt(lift2)}   （>1 で『サージ順位は素CCU順位に勝つ』＝芸あり）")
            print("  ※ lift≈1 なら『ただ大きい順』と大差なし＝順位設計の見直し材料。experimental は維持。")

            # ---------- [4] 推移＋例示 ----------
            print(f"\n[4] 直近 as-of の①率 推移（右ほど新しい）")
            tail = per_t[-10:]
            print("  " + "  ".join(f"{d.date()}:{_pct(int((r or 0)*n), n) if n else '—'}"
                                   for (d, r, n) in tail))
            if last_examples:
                print(f"\n  ── 最新 as-of の例（R=検出時6h p90 / base / 前向き中央値 / ピーク / 観測日数 / ①? / ②?）")
                print("     {:<28} {:>8} {:>8} {:>9} {:>9} {:>5} {:>4} {:>4}".format(
                    "name", "R", "base", "fwd_med", "fwd_max", "days", "①", "②"))
                for nm, R, b, fm, fx, dd, h1, h2 in last_examples[:EXAMPLES]:
                    print("     {:<28} {:>8} {:>8} {:>9} {:>9} {:>5} {:>4} {:>4}".format(
                        (nm or "")[:28], int(R) if R else 0,
                        "—" if b is None else int(b),
                        "—" if fm is None else int(fm),
                        "—" if fx is None else int(fx),
                        dd, "Y" if h1 else "-", "Y" if h2 else "-"))
    finally:
        conn.close()

    print("\n" + "=" * 78)
    print("この出力を Claude に共有してください。lift と①/②当たり率で『順位に芸があるか』を判定します。")
    print("v1母集団=サージ順位（boost/B1は未復元＝正直な割り切り）。しきい値・窓は env で可逆に調整可。")
    print("書き込み・ファイル変更・commit は一切していません（read-only固定）。")


if __name__ == "__main__":
    main()
