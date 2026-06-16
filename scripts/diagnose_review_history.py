#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
レビュー履歴 厚み点検（読み取り専用・印字のみ・本番非変更）
置き場所: scripts/diagnose_review_history.py

目的:
  「レビュー急増」ラベルを “絶対値 +50” から “そのゲームの平常比（自分比）” へ直すために、
  実データで次を測る。これ1回で『使える基準窓』と『自分比の効き目』まで分かる:
    [1] レビュー履歴は全体でいつから貯まっているか（= 自分比の基準にできる最長窓）
    [2] ランキングに出る注目ゲーム群（現CCU上位）の、各ゲームの履歴の厚み
        （何日ぶん / 何点 / どれくらいの間隔で取れているか）
    [3] いまの +50 ルールが大型ゲームに偏っている様子（過剰ラベルの実証）
    [4] 推奨「自分比（今週の増加が平常週ペースの何倍か）」の試算プレビュー。
        +50 と比べ、どの大型が正しく落ち、どの小型が拾えるかを複数の基準窓で対比。

  read-only（SELECTのみ・物理的に書けない固定）。書き込み・ファイル変更・commit は一切しない。
  出力（[1]〜[4] 全文）を Claude に共有 → 基準窓と暫定しきい値を確定 → 実装へ。

前提スキーマ（確定済）:
  review_snapshots(appid, total_reviews, recorded_at, ...)
  player_counts(appid, player_count, recorded_at)
  games(appid, name, ...)
"""
import os
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]

# --- 注目集合（ランキングに出る = 現CCU上位）の作り方 ---
ACTIVE_DAYS  = int(os.environ.get("ACTIVE_DAYS")  or "3")    # 直近この日数に観測がある = 稼働中とみなす
MIN_CURRENT  = int(os.environ.get("MIN_CURRENT")  or "100")  # 現CCUの下限（view02 既定に合わせる）
REL_N        = int(os.environ.get("REL_N")        or "150")  # 注目集合の件数（CCU上位）

# --- 今のルール（再現用） ---
REV_DAYS     = int(os.environ.get("CAUSE_REV_DAYS")  or "7")   # 「今週」の窓
REV_SURGE    = int(os.environ.get("CAUSE_REV_SURGE") or "50")  # 現行の絶対しきい値

# --- 推奨「自分比」の試算パラメータ（すべて暫定・env で上書き可） ---
BASELINE_DAYS_LIST = [int(x) for x in (os.environ.get("BASELINE_DAYS_LIST") or "28,21,14").split(",") if x.strip()]
REL_MULT     = float(os.environ.get("REL_MULT")  or "3")    # 平常週ペースの何倍で「急増」とみなすか
ABS_FLOOR    = int(os.environ.get("ABS_FLOOR")   or "15")   # 極小ゲームのノイズ除け（最低増加数）
EXAMPLES     = int(os.environ.get("EXAMPLES")    or "12")   # 例示行数


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


def latest_totals(cur, appids, cutoff_days=None):
    """relevant appids について、(cutoff_days 日前以前の) 最新スナップショットの total_reviews を
       {appid: int} で返す。cutoff_days=None なら全体の最新（=現在の総数）。"""
    if cutoff_days is None:
        cur.execute(
            "SELECT DISTINCT ON (appid) appid, total_reviews "
            "FROM review_snapshots WHERE appid = ANY(%s) "
            "ORDER BY appid, recorded_at DESC", (appids,))
    else:
        cur.execute(
            "SELECT DISTINCT ON (appid) appid, total_reviews "
            "FROM review_snapshots WHERE appid = ANY(%s) "
            "AND recorded_at <= now() - make_interval(days => %s) "
            "ORDER BY appid, recorded_at DESC", (appids, cutoff_days))
    return {a: (t or 0) for a, t in cur.fetchall()}


def main():
    print("=" * 72)
    print("レビュー履歴 厚み点検（読み取り専用・印字のみ・本番非変更）")
    print(f"params: ACTIVE_DAYS={ACTIVE_DAYS} MIN_CURRENT={MIN_CURRENT} REL_N={REL_N} "
          f"REV_DAYS={REV_DAYS} REV_SURGE={REV_SURGE}")
    print(f"        BASELINE_DAYS_LIST={BASELINE_DAYS_LIST} REL_MULT={REL_MULT} ABS_FLOOR={ABS_FLOOR}")
    print("=" * 72)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)  # 物理的に書けないよう固定
        with conn.cursor() as cur:

            # ---------- [1] 全体量・鮮度 ----------
            try:
                cur.execute(
                    "SELECT min(recorded_at), max(recorded_at), count(*), count(DISTINCT appid), "
                    "count(*) FILTER (WHERE recorded_at >= now() - make_interval(days => 1)) "
                    "FROM review_snapshots")
                mn, mx, total, n_app, rows24 = cur.fetchone()
                span_all = (mx - mn).days if (mn and mx) else None
                print("\n[1] レビュー履歴の全体量・鮮度（= 自分比の基準にできる最長窓）")
                print(f"  期間: {mn} 〜 {mx}（約 {span_all} 日ぶん）")
                print(f"  総行数={total}  レビュー履歴のある appid 数={n_app}  直近24h行数={rows24}")
                print("  ※『約N日ぶん』より長い基準窓は今は作れない。28日基準には『28日＋今週分』の履歴が要る。")
            except Exception as e:
                print(f"⚠ [1] overview 失敗: {type(e).__name__}: {e}")

            # ---------- relevant set: 現CCU上位 ----------
            rel = []
            try:
                cur.execute(
                    "WITH latest AS ("
                    "  SELECT DISTINCT ON (appid) appid, player_count "
                    "  FROM player_counts "
                    "  WHERE recorded_at >= now() - make_interval(days => %s) "
                    "  ORDER BY appid, recorded_at DESC) "
                    "SELECT appid, player_count FROM latest "
                    "WHERE player_count >= %s ORDER BY player_count DESC LIMIT %s",
                    (ACTIVE_DAYS, MIN_CURRENT, REL_N))
                rel = cur.fetchall()
            except Exception as e:
                print(f"⚠ relevant集合の取得 失敗: {type(e).__name__}: {e}")
            rel_ids = [a for a, _ in rel]
            ccu = {a: c for a, c in rel}

            names = {}
            if rel_ids:
                try:
                    cur.execute("SELECT appid, name FROM games WHERE appid = ANY(%s)", (rel_ids,))
                    names = {a: n for a, n in cur.fetchall()}
                except Exception as e:
                    print(f"⚠ name取得 失敗: {type(e).__name__}: {e}")

            def nm(a):
                return ((names.get(a) or f"appid:{a}")[:28])

            # ---------- per-game depth (relevant) ----------
            depth = {}  # appid -> (n_points, span_days)
            if rel_ids:
                try:
                    cur.execute(
                        "SELECT appid, count(*), min(recorded_at), max(recorded_at) "
                        "FROM review_snapshots WHERE appid = ANY(%s) GROUP BY appid", (rel_ids,))
                    for a, n, mn2, mx2 in cur.fetchall():
                        span = (mx2 - mn2).total_seconds() / 86400.0 if (mn2 and mx2) else 0.0
                        depth[a] = (n, span)
                except Exception as e:
                    print(f"⚠ depth取得 失敗: {type(e).__name__}: {e}")

            # ---------- [2] 注目集合の被覆・厚み ----------
            try:
                R = len(rel_ids)
                with_rev = len(depth)
                spans = [s for (_, s) in depth.values()]
                ns = [n for (n, _) in depth.values()]
                gaps = [(s / (n - 1)) for (n, s) in depth.values() if n >= 2]

                def ge(days):
                    return sum(1 for s in spans if s >= days)

                print(f"\n[2] 注目集合（現CCU上位 {R} 件・CCU≥{MIN_CURRENT}）のレビュー履歴の被覆・厚み")
                print(f"  レビュー履歴あり: {with_rev}/{R}（{_pct(with_rev, R)}）")
                print(f"  履歴の長さ別ゲーム数: ≥7日={ge(7)}  ≥14日={ge(14)}  ≥21日={ge(21)}  "
                      f"≥28日={ge(28)}  ≥35日={ge(35)}")
                print(f"  中央値: 履歴の長さ={_fmt(_med(spans), 1)}日  点数={_fmt(_med(ns), 1)}点  "
                      f"平均間隔(近似)={_fmt(_med(gaps), 2)}日")
                print("  ※『自分比(基準W日)』には各ゲームに W+7日 ぶんの履歴が要る。")
                print("    例: 28日基準なら ≥35日 の件数が、評価できるゲーム数の上限。少なすぎる窓は選べない。")
            except Exception as e:
                print(f"⚠ [2] 被覆 失敗: {type(e).__name__}: {e}")

            # ---------- maps for delta / baseline ----------
            latest, as_of_7d = {}, {}
            if rel_ids:
                latest = latest_totals(cur, rel_ids, None)
                as_of_7d = latest_totals(cur, rel_ids, REV_DAYS)

            delta7 = {}  # appid -> 今週(直近REV_DAYS日)の総レビュー増
            for a in rel_ids:
                if a in latest and a in as_of_7d:
                    delta7[a] = latest[a] - as_of_7d[a]

            # ---------- [3] 今の +50 ルールの挙動と大型偏重 ----------
            try:
                comput = len(delta7)
                short = len(rel_ids) - comput
                flagged = [a for a in delta7 if delta7[a] >= REV_SURGE]
                print(f"\n[3] 今の +{REV_SURGE} 絶対ルールの実データ挙動（過剰ラベルの実証）")
                print(f"  今週増加を計算できた: {comput}/{len(rel_ids)}（履歴<{REV_DAYS}日で計算不可: {short}件）")
                print(f"  +{REV_SURGE}以上で『急増』点灯: {len(flagged)}/{comput}（{_pct(len(flagged), comput)}）")
                rows = sorted(delta7.keys(), key=lambda a: latest.get(a, 0), reverse=True)[:EXAMPLES]
                print("  ── レビュー総数が多い順（大型ほど、何もなくても +50 を超えがち）")
                print("     {:<28} {:>10} {:>9} {:>6} {:>7}".format("name", "total_rev", "Δ7d", ">=50?", "ccu"))
                for a in rows:
                    print("     {:<28} {:>10} {:>9} {:>6} {:>7}".format(
                        nm(a), latest.get(a, 0), delta7[a],
                        "Y" if delta7[a] >= REV_SURGE else "-", ccu.get(a, 0)))
                print("  ※上位（大型）ばかりが Y なら、+50 は『伸び』でなく『大きさ』を測っている＝過剰ラベルの証拠。")
            except Exception as e:
                print(f"⚠ [3] +50挙動 失敗: {type(e).__name__}: {e}")

            # ---------- [4] 自分比プレビュー（複数の基準窓） ----------
            print(f"\n[4] 推奨『自分比』の試算（今週Δ ÷ 平常週ペース ≥ {REL_MULT} かつ Δ≥{ABS_FLOOR}）")
            print("    平常週ペース = (基準窓の間に増えたレビュー) ÷ (基準窓の週数)。基準窓は『今週』より前。")
            for W in BASELINE_DAYS_LIST:
                try:
                    base_start = latest_totals(cur, rel_ids, REV_DAYS + W)  # 今週開始の さらに W 日前
                    evaluable = 0
                    cur_flag = rel_flag = both = only_cur = only_rel = 0
                    rows_drop = []   # 今は急増だが自分比では平常（過剰の正体）
                    rows_gain = []   # 自分比なら拾うが今は見逃し（小型の本物）
                    for a in rel_ids:
                        d = delta7.get(a)
                        if d is None or a not in as_of_7d or a not in base_start:
                            continue  # 履歴不足（この窓では評価不可）
                        evaluable += 1
                        base_gain = as_of_7d[a] - base_start[a]      # 基準窓[start..今週開始]の増加
                        bw = base_gain / (W / 7.0)                   # 平常週ペース
                        ratio = (d / bw) if bw > 0 else None         # 平常≈0/負 は別扱い
                        cf = d >= REV_SURGE
                        rf = (d >= ABS_FLOOR) and ((ratio is not None and ratio >= REL_MULT) or bw <= 0)
                        cur_flag += cf
                        rel_flag += rf
                        if cf and rf:
                            both += 1
                        if cf and not rf:
                            only_cur += 1
                            rows_drop.append((a, latest.get(a, 0), d, bw, ratio))
                        if rf and not cf:
                            only_rel += 1
                            rows_gain.append((a, latest.get(a, 0), d, bw, ratio))

                    print(f"\n  ── 基準窓 {W}日（履歴 ≥{W + REV_DAYS}日 が必要）: 評価できたゲーム {evaluable}/{len(rel_ids)}")
                    if evaluable == 0:
                        print(f"    評価0件＝この窓は今は履歴不足で使えない（[1][2]の蓄積待ち、またはもっと短い窓へ）。")
                        continue
                    print(f"    現+{REV_SURGE}で点灯={cur_flag}  自分比で点灯={rel_flag}  両方={both}  "
                          f"現だけ(=自分比が落とす)={only_cur}  自分比だけ(=現が見逃す)={only_rel}")
                    if rows_drop:
                        rows_drop.sort(key=lambda r: r[1], reverse=True)
                        print("    ・『今は急増だが自分比では平常』例（過剰ラベルの正体・total大きい順）:")
                        for a, tr, d, bw, ra in rows_drop[:EXAMPLES]:
                            print("        {:<28} total={:>8} Δ7d={:>7} 平常週={:>7} 倍率={}".format(
                                nm(a), tr, d, _fmt(bw, 1), _fmt(ra, 1)))
                    if rows_gain:
                        rows_gain.sort(key=lambda r: ((r[4] is None), -(r[4] or 0)))
                        print("    ・『自分比なら拾うが今は見逃し』例（小型の本物の急増・倍率高い順）:")
                        for a, tr, d, bw, ra in rows_gain[:EXAMPLES]:
                            print("        {:<28} total={:>8} Δ7d={:>7} 平常週={:>7} 倍率={}".format(
                                nm(a), tr, d, _fmt(bw, 1), "∞(平常≈0)" if ra is None else _fmt(ra, 1)))
                except Exception as e:
                    print(f"  ⚠ 基準窓 {W}日 失敗: {type(e).__name__}: {e}")

    finally:
        conn.close()

    print("\n" + "=" * 72)
    print("この出力（[1]〜[4] 全文）を Claude に共有してください。")
    print("→ [1][2] で『使える基準窓』を、[3][4] で『自分比の効き目と暫定しきい値』を確定します。")
    print("書き込み・ファイル変更・commit は一切していません（read-only固定）。")


if __name__ == "__main__":
    main()
