#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CCU履歴 厚み点検 ＋ ①持続/②追随 as-of ドライラン（読み取り専用・印字のみ・本番非変更）
置き場所: scripts/diagnose_ccu_history.py

目的:
  バックテストの当たり定義を「①持続（主）＋②追随（副）」に確定した（HANDOFF §8 / 叩き台 2026-07-06）。
  着手前に、CCU履歴(player_counts)が ①持続の採点に足りるかを実データで測り、しきい値感をつかむ。
  これ1回で『as-of で遡れる範囲』『採点母集団の被覆・厚み』『サンプリング密度』『①②の効き目プレビュー』が分かる:
    [1] CCU履歴は全体でいつから貯まっているか（= as-of で遡れる最長範囲）
    [2] ランキングに出る注目ゲーム群（現CCU上位）の履歴の厚み（採点に要る BASE+GAP+H 日を満たす件数）
    [3] サンプリング密度: 日次被覆（①持続の材料）＋ 直近6h密度（②追随／p90鋭さの材料）
    [4] ①持続 ＋ ②追随 の as-of ドライラン（t0 = 最新 − H 日・厳密に無リーク）:
        当時の値だけで baseline/recent を復元 → 前向き窓 [t0, t0+H] で持続/追随を採点 → hit率としきい値感を見る。

  窓の定義は view02_rank_v2.py の本番と一致させてある（recent=直近6hのp90 / baseline=14日窓の中央値・1日gap）。
  すなわち now() を t0 に差し替えただけ＝as-of。前向き窓だけが (t0, t0+H]。未来を1点も混ぜない（リーク防止）。

  read-only（SELECTのみ・物理的に書けない固定）。書き込み・ファイル変更・commit は一切しない。
  出力（[1]〜[4] 全文）を Claude に共有 → 採点可否と暫定しきい値を確定 → as-of再構成ランナー／採点器の実装へ。

前提スキーマ（確定済）:
  player_counts(appid, player_count, recorded_at)
  games(appid, name, ...)
"""
import os
import datetime
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]

# --- 注目集合（ランキングに出る = 現CCU上位）の作り方（view02 既定に合わせる） ---
ACTIVE_DAYS  = int(os.environ.get("ACTIVE_DAYS")  or "3")    # 直近この日数に観測がある = 稼働中
MIN_CURRENT  = int(os.environ.get("MIN_CURRENT")  or "100")  # 現CCUの下限（view02 既定）
REL_N        = int(os.environ.get("REL_N")        or "50")   # 点検母集団（CCU上位）。※当たり母集団=Top-N(既定30)より少し広く見る

# --- 窓（view02_rank_v2.py の本番と一致・as-of 採点でもこの定義を流用） ---
BASE_DAYS    = int(os.environ.get("BASE_DAYS")    or "14")   # 平常窓の長さ
GAP_DAYS     = int(os.environ.get("GAP_DAYS")     or "1")    # 平常窓と現在の間の緩衝
RECENT_HOURS = int(os.environ.get("RECENT_HOURS") or "6")    # 直近窓（recent_value=p90 の窓）
RECENT_Q     = float(os.environ.get("RECENT_Q")   or "0.9")  # 直近窓の高分位（鋭さ）
MIN_POINTS   = int(os.environ.get("MIN_POINTS")   or "5")    # baseline の最低点数（view02 既定）

# --- バックテスト前向き窓・当たりしきい値（すべて暫定・env で上書き可） ---
H_DAYS           = int(os.environ.get("H_DAYS")           or "7")    # 前向き窓（当たりを測る先の日数）
FORWARD_MIN_DAYS = int(os.environ.get("FORWARD_MIN_DAYS") or "3")    # 前向き窓に最低これだけの観測日が要る
PERSIST_FRAC     = float(os.environ.get("PERSIST_FRAC")   or "0.6")  # ①持続: 窓中央値 ≥ この割合 × R_t で当たり
RISE_MULT        = float(os.environ.get("RISE_MULT")      or "1.3")  # ②追随: 窓ピーク ≥ この倍率 × R_t で当たり
EXAMPLES         = int(os.environ.get("EXAMPLES")         or "12")   # 例示行数


def _fmt(x, nd=2):
    return "—" if x is None else f"{x:.{nd}f}"


def _pct(a, b):
    return "—" if not b else f"{100.0 * a / b:.0f}%"


def _med(xs):
    if not xs:
        return None
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


def top_ccu(cur, cutoff_ts, active_days, min_current, n):
    """現CCU上位（cutoff_ts=None は現在／datetime指定はその時刻 as-of）を [(appid, ccu)] で返す。"""
    if cutoff_ts is None:
        cur.execute(
            "WITH latest AS ("
            "  SELECT DISTINCT ON (appid) appid, player_count AS ccu "
            "  FROM player_counts "
            "  WHERE recorded_at >= now() - make_interval(days => %s) "
            "  ORDER BY appid, recorded_at DESC) "
            "SELECT appid, ccu FROM latest WHERE ccu >= %s ORDER BY ccu DESC LIMIT %s",
            (active_days, min_current, n))
    else:
        cur.execute(
            "WITH latest AS ("
            "  SELECT DISTINCT ON (appid) appid, player_count AS ccu "
            "  FROM player_counts "
            "  WHERE recorded_at <= %s AND recorded_at >= %s - make_interval(days => %s) "
            "  ORDER BY appid, recorded_at DESC) "
            "SELECT appid, ccu FROM latest WHERE ccu >= %s ORDER BY ccu DESC LIMIT %s",
            (cutoff_ts, cutoff_ts, active_days, min_current, n))
    return cur.fetchall()


def names_of(cur, ids):
    if not ids:
        return {}
    try:
        cur.execute("SELECT appid, name FROM games WHERE appid = ANY(%s)", (ids,))
        return {a: n for a, n in cur.fetchall()}
    except Exception as e:
        print(f"⚠ name取得 失敗: {type(e).__name__}: {e}")
        return {}


def main():
    need_days = BASE_DAYS + GAP_DAYS + H_DAYS  # as-of 1点を採点するのに要る最短履歴
    print("=" * 76)
    print("CCU履歴 厚み点検 ＋ ①持続/②追随 as-of ドライラン（読み取り専用・印字のみ・本番非変更）")
    print(f"params: MIN_CURRENT={MIN_CURRENT} REL_N={REL_N} BASE_DAYS={BASE_DAYS} GAP_DAYS={GAP_DAYS} "
          f"RECENT_HOURS={RECENT_HOURS} RECENT_Q={RECENT_Q} MIN_POINTS={MIN_POINTS}")
    print(f"        H_DAYS={H_DAYS} FORWARD_MIN_DAYS={FORWARD_MIN_DAYS} "
          f"PERSIST_FRAC={PERSIST_FRAC} RISE_MULT={RISE_MULT}（すべて暫定）")
    print(f"        採点に要る最短履歴 = BASE+GAP+H = {need_days} 日")
    print("=" * 76)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)  # 物理的に書けないよう固定
        with conn.cursor() as cur:

            # ---------- [1] 全体量・鮮度 ----------
            mx_all = None
            span_all = None
            try:
                cur.execute(
                    "SELECT min(recorded_at), max(recorded_at), count(*), count(DISTINCT appid), "
                    "count(*) FILTER (WHERE recorded_at >= now() - make_interval(days => 1)) "
                    "FROM player_counts")
                mn, mx_all, total, n_app, rows24 = cur.fetchone()
                span_all = (mx_all - mn).days if (mn and mx_all) else None
                usable = (span_all - need_days) if span_all is not None else None
                print("\n[1] CCU履歴の全体量・鮮度（= as-of で遡れる最長範囲）")
                print(f"  期間: {mn} 〜 {mx_all}（約 {span_all} 日ぶん）")
                print(f"  総行数={total}  CCU履歴のある appid 数={n_app}  直近24h行数={rows24}")
                print(f"  → as-of 採点できる日数の目安 ≈ span − (BASE+GAP+H) = {usable} 日ぶん")
                print("    （各 as-of 日 t は『t の前に14日＋t の後に7日』が要る。span がこれを下回る窓は採点不可）")
            except Exception as e:
                print(f"⚠ [1] overview 失敗: {type(e).__name__}: {e}")

            # ---------- relevant set: 現CCU上位（[2][3] 用） ----------
            rel = []
            try:
                rel = top_ccu(cur, None, ACTIVE_DAYS, MIN_CURRENT, REL_N)
            except Exception as e:
                print(f"⚠ relevant集合の取得 失敗: {type(e).__name__}: {e}")
            rel_ids = [a for a, _ in rel]
            ccu = {a: c for a, c in rel}
            names = names_of(cur, rel_ids)

            def nm(a):
                return ((names.get(a) or f"appid:{a}")[:28])

            # ---------- per-game 全期間の厚み ----------
            depth = {}  # appid -> (n_points, span_days, distinct_days)
            if rel_ids:
                try:
                    cur.execute(
                        "SELECT appid, count(*), min(recorded_at), max(recorded_at), "
                        "count(DISTINCT date_trunc('day', recorded_at)) "
                        "FROM player_counts WHERE appid = ANY(%s) GROUP BY appid", (rel_ids,))
                    for a, n, mn2, mx2, dd in cur.fetchall():
                        span = (mx2 - mn2).total_seconds() / 86400.0 if (mn2 and mx2) else 0.0
                        depth[a] = (n, span, dd)
                except Exception as e:
                    print(f"⚠ depth取得 失敗: {type(e).__name__}: {e}")

            # ---------- [2] 注目集合の被覆・厚み ----------
            try:
                R = len(rel_ids)
                spans = [s for (_, s, _) in depth.values()]
                ns = [n for (n, _, _) in depth.values()]

                def ge(days):
                    return sum(1 for s in spans if s >= days)

                print(f"\n[2] 注目集合（現CCU上位 {R} 件・CCU≥{MIN_CURRENT}）のCCU履歴の被覆・厚み")
                print(f"  履歴の長さ別ゲーム数: ≥7日={ge(7)}  ≥14日={ge(14)}  "
                      f"≥{need_days}日(採点可)={ge(need_days)}  ≥28日={ge(28)}")
                print(f"  中央値: 履歴の長さ={_fmt(_med(spans), 1)}日  点数={_fmt(_med(ns), 1)}点")
                print(f"  ※ ①持続を1件採点するには各ゲームに {need_days}日 ぶんの履歴が要る。")
                print(f"    ≥{need_days}日 の件数が『いま as-of 採点できるゲーム数』の上限。")
            except Exception as e:
                print(f"⚠ [2] 被覆 失敗: {type(e).__name__}: {e}")

            # ---------- [3] サンプリング密度（日次＋6h） ----------
            try:
                # 日次被覆（全期間）: distinct_days / span_days
                daily_cov = [dd / s for (_, s, dd) in depth.values() if s and s >= 1.0]
                # 直近14日の6h密度
                b6 = {}
                if rel_ids and mx_all is not None:
                    cur.execute(
                        "SELECT appid, count(*), "
                        "count(DISTINCT floor(extract(epoch FROM recorded_at) / 21600)), "
                        "count(DISTINCT date_trunc('day', recorded_at)) "
                        "FROM player_counts "
                        "WHERE appid = ANY(%s) AND recorded_at >= %s - make_interval(days => 14) "
                        "GROUP BY appid", (rel_ids, mx_all))
                    for a, n14, nb6, d14 in cur.fetchall():
                        b6[a] = (n14, nb6, d14)
                pts_per_6h = [n14 / nb6 for (n14, nb6, _) in b6.values() if nb6]
                cov_6h = [nb6 / 56.0 for (_, nb6, _) in b6.values()]     # 14日×4バケット=56
                cov_d14 = [d14 / 14.0 for (_, _, d14) in b6.values()]

                print("\n[3] サンプリング密度（①持続は日次被覆／②追随・p90鋭さは6h密度が効く）")
                print(f"  日次被覆（全期間）中央値 = {_fmt(_med(daily_cov), 2)}"
                      f"（1.0 に近いほど『毎日欠けなく』観測。前向き窓の穴埋めの指標）")
                print(f"  直近14日の 1観測6hバケットあたり点数 中央値 = {_fmt(_med(pts_per_6h), 1)}"
                      f"（>1 なら p90 が意味を持つ。≈1 だと recent_value≈current_ccu＝②の鋭さが鈍る）")
                print(f"  直近14日の 6hバケット被覆 中央値 = {_pct(int(1000 * (_med(cov_6h) or 0)), 1000)}"
                      f"（56バケット中どれだけ埋まっているか）")
                print(f"  直近14日の 日次被覆 中央値 = {_pct(int(1000 * (_med(cov_d14) or 0)), 1000)}"
                      f"（前向き窓 {H_DAYS}日 に {FORWARD_MIN_DAYS}日以上 入るかの目安）")
                print("  ※ dense_sweep(A'案)が効いている集合は6h密度が高いはず。日次被覆が低いと①の窓が穴だらけになる。")
            except Exception as e:
                print(f"⚠ [3] 密度 失敗: {type(e).__name__}: {e}")

            # ---------- [4] ①持続 ＋ ②追随 as-of ドライラン ----------
            print(f"\n[4] ①持続 ＋ ②追随 の as-of ドライラン（t0 = 最新 − {H_DAYS}日・厳密に無リーク）")
            try:
                if mx_all is None:
                    raise RuntimeError("最新時刻が取れず t0 を決められない")
                t0 = mx_all - datetime.timedelta(days=H_DAYS)   # 前向き窓が丸ごと閉じている直近の as-of 日
                t1 = mx_all                                     # t0 + H
                print(f"  as-of t0 = {t0}  /  前向き窓 (t0, t0+{H_DAYS}日] = (t0, {t1}]")

                # 母集団 = t0 時点の CCU上位（未来を混ぜない）
                pop = top_ccu(cur, t0, ACTIVE_DAYS, MIN_CURRENT, REL_N)
                pop_ids = [a for a, _ in pop]
                ccu0 = {a: c for a, c in pop}
                pnames = names_of(cur, pop_ids)

                def pnm(a):
                    return ((pnames.get(a) or f"appid:{a}")[:28])

                rows = {}
                if pop_ids:
                    cur.execute(
                        "SELECT appid, "
                        # baseline: [t0-BASE, t0-GAP) の中央値 ＋ 点数
                        " percentile_cont(0.5) WITHIN GROUP (ORDER BY player_count) "
                        "   FILTER (WHERE recorded_at <  %(t0)s - make_interval(days => %(gap)s) "
                        "             AND recorded_at >= %(t0)s - make_interval(days => %(base)s)) AS base_med, "
                        " count(*) FILTER (WHERE recorded_at <  %(t0)s - make_interval(days => %(gap)s) "
                        "                   AND recorded_at >= %(t0)s - make_interval(days => %(base)s)) AS base_n, "
                        # recent: (t0-RECENT_HOURS, t0] の p90 ＋ 点数
                        " percentile_cont(%(rq)s) WITHIN GROUP (ORDER BY player_count) "
                        "   FILTER (WHERE recorded_at <= %(t0)s "
                        "             AND recorded_at > %(t0)s - make_interval(hours => %(rh)s)) AS r_p90, "
                        " count(*) FILTER (WHERE recorded_at <= %(t0)s "
                        "                   AND recorded_at > %(t0)s - make_interval(hours => %(rh)s)) AS r_n, "
                        # forward: (t0, t1] の中央値・ピーク・点数・観測日数
                        " percentile_cont(0.5) WITHIN GROUP (ORDER BY player_count) "
                        "   FILTER (WHERE recorded_at > %(t0)s AND recorded_at <= %(t1)s) AS fwd_med, "
                        " max(player_count) FILTER (WHERE recorded_at > %(t0)s AND recorded_at <= %(t1)s) AS fwd_max, "
                        " count(*) FILTER (WHERE recorded_at > %(t0)s AND recorded_at <= %(t1)s) AS fwd_n, "
                        " count(DISTINCT date_trunc('day', recorded_at)) "
                        "   FILTER (WHERE recorded_at > %(t0)s AND recorded_at <= %(t1)s) AS fwd_days "
                        "FROM player_counts "
                        "WHERE appid = ANY(%(ids)s) "
                        "  AND recorded_at >= %(t0)s - make_interval(days => %(base)s) "
                        "  AND recorded_at <= %(t1)s "
                        "GROUP BY appid",
                        {"t0": t0, "t1": t1, "gap": GAP_DAYS, "base": BASE_DAYS,
                         "rq": RECENT_Q, "rh": RECENT_HOURS, "ids": pop_ids})
                    for a, bmed, bn, rp90, rn, fmed, fmax, fn, fdays in cur.fetchall():
                        rows[a] = dict(bmed=bmed, bn=bn or 0, rp90=rp90, rn=rn or 0,
                                       fmed=fmed, fmax=fmax, fn=fn or 0, fdays=fdays or 0)

                scorable = 0
                no_base = no_fwd = 0
                hit1 = hit2 = 0
                ex = []
                for a in pop_ids:
                    d = rows.get(a)
                    if not d:
                        no_fwd += 1
                        continue
                    # R_t0 = 直近6hのp90（無ければ t0 時点の現CCU＝production の COALESCE と同じ）
                    R = d["rp90"] if d["rp90"] is not None else ccu0.get(a)
                    if d["bn"] < MIN_POINTS or d["bmed"] is None:
                        no_base += 1
                        continue
                    if d["fn"] == 0 or R is None or R <= 0:
                        no_fwd += 1
                        continue
                    scorable += 1
                    thr = max((d["bmed"] or 0) * 1.5, PERSIST_FRAC * R)   # ①の高止まりライン（参考）
                    h1 = (d["fmed"] is not None and d["fmed"] >= PERSIST_FRAC * R
                          and d["fdays"] >= FORWARD_MIN_DAYS)
                    h2 = (d["fmax"] is not None and d["fmax"] >= RISE_MULT * R)
                    hit1 += h1
                    hit2 += h2
                    ex.append((a, R, d["bmed"], d["fmed"], d["fmax"], d["fdays"], h1, h2, thr))

                print(f"  母集団（t0時点のCCU上位）= {len(pop_ids)} 件")
                print(f"  採点できた（baseline≥{MIN_POINTS}点 かつ 前向き窓に観測あり）= {scorable} 件"
                      f"（baseline不足={no_base} / 前向き窓が空={no_fwd}）")
                if scorable:
                    print(f"  ①持続 当たり: {hit1}/{scorable}（{_pct(hit1, scorable)}）"
                          f"  ＝窓中央値 ≥ {PERSIST_FRAC}×R かつ 観測 ≥{FORWARD_MIN_DAYS}日")
                    print(f"  ②追随 当たり: {hit2}/{scorable}（{_pct(hit2, scorable)}）"
                          f"  ＝窓ピーク ≥ {RISE_MULT}×R（副指標）")
                    print("  ※『採点母集団=CCU上位』は簡易版（本番は view02 のサージ上位）。ここは計算が通るか＋しきい値感の確認。")
                    print("    ①の厳密版『≥{}日 が max(B×1.5, {}×R) 以上』は採点器で実装。ここは中央値近似のプレビュー。"
                          .format(FORWARD_MIN_DAYS, PERSIST_FRAC))
                    ex.sort(key=lambda r: (r[1] or 0), reverse=True)
                    print("\n  ── 例（R_t0=検出時の直近6h p90 / 平常 / 前向き中央値 / 前向きピーク / 観測日数 / ①? / ②?）")
                    print("     {:<28} {:>8} {:>8} {:>9} {:>9} {:>5} {:>4} {:>4}".format(
                        "name", "R_t0", "base", "fwd_med", "fwd_max", "days", "①", "②"))
                    for a, R, bmed, fmed, fmax, fdays, h1, h2, thr in ex[:EXAMPLES]:
                        print("     {:<28} {:>8} {:>8} {:>9} {:>9} {:>5} {:>4} {:>4}".format(
                            pnm(a), int(R),
                            "—" if bmed is None else int(bmed),
                            "—" if fmed is None else int(fmed),
                            "—" if fmax is None else int(fmax),
                            fdays, "Y" if h1 else "-", "Y" if h2 else "-"))
                else:
                    print("  採点0件＝この t0 では履歴不足。span を伸ばす（[1] の蓄積待ち）か H を短くして再確認。")
            except Exception as e:
                print(f"⚠ [4] ドライラン 失敗: {type(e).__name__}: {e}")

    finally:
        conn.close()

    print("\n" + "=" * 76)
    print("この出力（[1]〜[4] 全文）を Claude に共有してください。")
    print("→ [1][2][3] で『いま as-of 採点できるか・母集団と密度は足りるか』を、")
    print("  [4] で『①持続/②追随の計算が通るか＋暫定しきい値の当たり率感』を確定します。")
    print("次段: 確定できたら as-of再構成ランナー（view02 を ≤t で回す）＋採点器（①＋②副指標＋lift）の設計へ。")
    print("書き込み・ファイル変更・commit は一切していません（read-only固定）。")


if __name__ == "__main__":
    main()
