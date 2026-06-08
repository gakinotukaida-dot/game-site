"""
view02 ランキング（A1サージ ＋ 推定きっかけ）── 読み取り専用・印字のみ ── 2026-06-08
================================================================
役割：本番 Neon を「読むだけ」で、
  (1) 各ゲームの「いつもより伸び」スコア（自己比・中央値基準・小標本収縮・頑健z）を出し、
  (2) collector(dense_sweep) と同一定義の is_riser / is_launch も併記し（＝定義の整合・二重定義防止）、
  (3) 既存テーブルから推定きっかけ（セール/更新/新作/無料/レビュー増）を確信度つきで付ける。
Twitch は未収集＝「配信」は対象外（後段）。当てられない時は「原因不明」と正直に出す。

安全性：接続を read-only に固定・SELECT のみ・commit/ファイル書込なし＝完全に戻せる。本番 export/箱/テーブルに触れない。
設計：Q_MAIN は player_counts と games だけに依存（最も確実）。きっかけ系（価格/告知/無料/レビュー）は
      各々 try/except の別クエリにして、列名差があってもランキング本体は壊れないようにする（防御的）。
整合(B)：is_riser/is_launch は dense_sweep.py と同じ式・同じ既定値。
"""

import os
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
# --- サージ（自己比）---
BASE_DAYS   = int(os.environ.get("BASE_DAYS")   or "14")   # 平常値の窓（dense と同じ14日）
GAP_DAYS    = int(os.environ.get("GAP_DAYS")    or "1")    # 直近を平常値から除外（自己汚染防止）
N0          = float(os.environ.get("N0")        or "10")   # 収縮の強さ
MIN_CURRENT = int(os.environ.get("MIN_CURRENT") or "100")
MIN_POINTS  = int(os.environ.get("MIN_POINTS")  or "5")
TOP_N       = int(os.environ.get("TOP_N")       or "30")
# --- dense_sweep と同一の riser/launch 定義（整合）---
RECENT_HOURS    = int(os.environ.get("RISER_RECENT_HOURS") or "6")
RISER_BASE_DAYS = int(os.environ.get("RISER_BASE_DAYS")    or "14")
RISER_MIN_BASE  = int(os.environ.get("RISER_MIN_BASE_OBS") or "2")
RISER_MULT      = float(os.environ.get("RISER_MULT")      or "3")
RISER_ABS_ADD   = int(os.environ.get("RISER_ABS_ADD")     or "200")
RISER_ABS_FLOOR = int(os.environ.get("RISER_ABS_FLOOR")   or "200")
LAUNCH_DAYS     = int(os.environ.get("LAUNCH_DAYS")       or "14")
# --- きっかけ ---
NEWS_DAYS   = int(os.environ.get("CAUSE_NEWS_DAYS") or "7")    # 直近この日数の告知を「更新」とみなす
REV_DAYS    = int(os.environ.get("CAUSE_REV_DAYS")  or "7")    # レビュー増の比較期間
REV_SURGE   = int(os.environ.get("CAUSE_REV_SURGE") or "50")   # この件数以上増えたら「レビュー急増」(暫定)

P = {"gap_days": GAP_DAYS, "base_days": BASE_DAYS, "n0": N0,
     "min_current": MIN_CURRENT, "min_points": MIN_POINTS, "top_n": TOP_N,
     "recent_h": RECENT_HOURS, "riser_base_days": RISER_BASE_DAYS,
     "riser_min_base": RISER_MIN_BASE, "riser_mult": RISER_MULT,
     "riser_abs_add": RISER_ABS_ADD, "riser_abs_floor": RISER_ABS_FLOOR,
     "launch_days": LAUNCH_DAYS}

# player_counts と games のみ（最も確実）。is_riser/is_launch は dense_sweep と同式。
Q_MAIN = """
WITH latest AS (
  SELECT DISTINCT ON (appid) appid, player_count AS current_ccu, recorded_at AS last_at
  FROM player_counts ORDER BY appid, recorded_at DESC
),
base AS (
  SELECT appid,
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY player_count) AS baseline,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY player_count) AS q1,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY player_count) AS q3,
    count(*) AS n_points
  FROM player_counts
  WHERE recorded_at <  now() - make_interval(days => %(gap_days)s)
    AND recorded_at >= now() - make_interval(days => %(base_days)s)
  GROUP BY appid
),
win AS (
  SELECT appid,
    max(player_count) FILTER (WHERE recorded_at >= now() - make_interval(hours => %(recent_h)s)) AS recent_max,
    avg(player_count) FILTER (WHERE recorded_at <  now() - make_interval(hours => %(recent_h)s)) AS base_avg,
    count(*)          FILTER (WHERE recorded_at <  now() - make_interval(hours => %(recent_h)s)) AS base_n
  FROM player_counts WHERE recorded_at >= now() - make_interval(days => %(riser_base_days)s)
  GROUP BY appid
)
SELECT l.appid, g.name, g.release_date, g.coming_soon,
  l.current_ccu, l.last_at, b.baseline, b.n_points,
  l.current_ccu::float / NULLIF(b.baseline,0)                AS raw_ratio,
  1 + (b.n_points::float / (b.n_points + %(n0)s))
      * (l.current_ccu::float / NULLIF(b.baseline,0) - 1)    AS shrunk_ratio,
  (l.current_ccu - b.baseline) / NULLIF(b.q3 - b.q1, 0)      AS robust_z,
  (w.recent_max IS NOT NULL AND w.base_avg IS NOT NULL AND w.base_n >= %(riser_min_base)s
    AND w.recent_max >= GREATEST(w.base_avg * %(riser_mult)s, w.base_avg + %(riser_abs_add)s)
    AND w.recent_max >= %(riser_abs_floor)s)                 AS is_riser,
  (g.coming_soon IS FALSE AND g.release_date IS NOT NULL
    AND g.release_date >= (now()::date - %(launch_days)s))   AS is_launch
FROM latest l
JOIN games g ON g.appid = l.appid
LEFT JOIN base b ON b.appid = l.appid
LEFT JOIN win  w ON w.appid = l.appid
WHERE l.current_ccu >= %(min_current)s
  AND b.n_points    >= %(min_points)s
ORDER BY shrunk_ratio DESC NULLS LAST
LIMIT %(top_n)s;
"""


def _cause_sets(cur, ids):
    """ランキング上位 ids について、きっかけの材料を既存テーブルから集める。
    各ブロックは try/except＝列名差があってもランキングは壊さない（その原因だけ欠ける）。"""
    sale, news, free, revd = {}, set(), set(), {}
    # セール：最新 price_snapshots の discount_percent
    try:
        cur.execute(
            "SELECT DISTINCT ON (appid) appid, discount_percent FROM price_snapshots "
            "WHERE appid = ANY(%s) ORDER BY appid, recorded_at DESC", (ids,))
        for a, d in cur.fetchall():
            sale[a] = d or 0
    except Exception as e:
        print(f"  ⚠ cause[sale] skip: {type(e).__name__}: {e}")
    # 更新/告知：直近 NEWS_DAYS 日の announcements
    try:
        cur.execute(
            "SELECT DISTINCT appid FROM announcements "
            "WHERE appid = ANY(%s) AND published_at >= now() - make_interval(days => %s)",
            (ids, NEWS_DAYS))
        news = {r[0] for r in cur.fetchall()}
    except Exception as e:
        print(f"  ⚠ cause[news] skip: {type(e).__name__}: {e}")
    # 無料配布：有効な free_promos
    try:
        cur.execute(
            "SELECT DISTINCT appid FROM free_promos WHERE appid = ANY(%s) "
            "AND (discount_end_date IS NULL OR discount_end_date >= now())", (ids,))
        free = {r[0] for r in cur.fetchall()}
    except Exception as e:
        print(f"  ⚠ cause[free] skip: {type(e).__name__}: {e}")
    # レビュー急増：最新 total_reviews − REV_DAYS日前の total_reviews
    try:
        cur.execute(
            "WITH rn AS (SELECT DISTINCT ON (appid) appid, total_reviews FROM review_snapshots "
            "            WHERE appid = ANY(%s) ORDER BY appid, recorded_at DESC), "
            "     ro AS (SELECT DISTINCT ON (appid) appid, total_reviews FROM review_snapshots "
            "            WHERE appid = ANY(%s) AND recorded_at <= now() - make_interval(days => %s) "
            "            ORDER BY appid, recorded_at DESC) "
            "SELECT rn.appid, rn.total_reviews - COALESCE(ro.total_reviews, rn.total_reviews) "
            "FROM rn LEFT JOIN ro USING (appid)", (ids, ids, REV_DAYS))
        for a, dl in cur.fetchall():
            revd[a] = dl or 0
    except Exception as e:
        print(f"  ⚠ cause[review] skip: {type(e).__name__}: {e}")
    return sale, news, free, revd


def _cause_label(appid, row, sale, news, free, revd):
    causes = []
    if sale.get(appid, 0) and sale[appid] > 0:
        causes.append(f"セール{sale[appid]}%")
    if appid in news:
        causes.append("更新/告知")
    if row["is_launch"]:
        causes.append("新作")
    if appid in free:
        causes.append("無料配布")
    if revd.get(appid, 0) >= REV_SURGE:
        causes.append(f"レビュー急増(+{revd[appid]})")
    # 確信度：強い伸び(z>=2)＋原因1つ=高 / 原因あり=中 / 原因なし or 履歴薄=低
    z = row["robust_z"] or 0
    n = row["n_points"] or 0
    if not causes:
        return "原因不明", ("低" if (z < 2 or n < 7) else "中")
    conf = "高" if (z >= 2 and n >= 7 and len(causes) <= 2) else "中"
    if n < 5:
        conf = "低"
    return " + ".join(causes), conf


def main():
    print("=" * 78)
    print("view02 ランキング（A1サージ＋推定きっかけ・読み取り専用・本番非変更）")
    print(f"params: BASE_DAYS={BASE_DAYS} GAP={GAP_DAYS} N0={N0} MIN_CURRENT={MIN_CURRENT} "
          f"MIN_POINTS={MIN_POINTS} TOP_N={TOP_N} / riser(dense整合): mult={RISER_MULT} "
          f"recent_h={RECENT_HOURS} / cause: news{NEWS_DAYS}d rev{REV_DAYS}d>={REV_SURGE}")
    print("=" * 78)
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(Q_MAIN, P)
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if not rows:
                print("0件。MIN_CURRENT/MIN_POINTS を緩めるか窓を短くして再診断。")
                return
            ids = [r["appid"] for r in rows]
            sale, news, free, revd = _cause_sets(cur, ids)

            print(f"\n伸び順（shrunk_ratio）上位 {len(rows)} 件：")
            print("  name                        現在     平常   倍率   z   riser launch  推定きっかけ / 確信度")
            n_unknown = 0
            for r in rows:
                label, conf = _cause_label(r["appid"], r, sale, news, free, revd)
                if label == "原因不明":
                    n_unknown += 1
                nm = (r["name"] or str(r["appid"]))[:26].ljust(26)
                base = "—" if r["baseline"] is None else f"{r['baseline']:.0f}"
                ratio = "—" if r["shrunk_ratio"] is None else f"{r['shrunk_ratio']:.2f}"
                z = "—" if r["robust_z"] is None else f"{r['robust_z']:.1f}"
                riser = "Y" if r["is_riser"] else "-"
                launch = "Y" if r["is_launch"] else "-"
                print(f"  {nm} {str(r['current_ccu']).rjust(8)} {base.rjust(7)} "
                      f"{ratio.rjust(5)} {z.rjust(4)}  {riser}     {launch}      {label} / {conf}")
            print(f"\n原因不明: {n_unknown}/{len(rows)} 件（正直に出す）。is_riser は dense_sweep と同一定義。")
    finally:
        conn.close()
    print("\n" + "=" * 78)
    print("この出力を共有 → 推定きっかけの精度と暫定パラメータを調整 → 出力JSON設計へ。書込なし。")


if __name__ == "__main__":
    main()
