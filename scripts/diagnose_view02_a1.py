"""
view02 A1 診断（読み取り専用・印字のみ）── 2026-06-08
================================================================
役割：本番 Neon を「読むだけ」で、view02「いつもより伸び」の A1 設計
      （基準窓を伸びから分離・7倍数窓・小標本収縮・頑健z）を実データで点検し、
      暫定パラメータ（窓/除外/収縮/有意水準/下限）を確定するための数字を出す。

安全性（厳守）：
  - 接続を **read-only セッションに固定**（set_session readonly=True）＝物理的に書けない。
  - SELECT のみ・commit しない・**ファイルもリポジトリも一切書き換えない**（標準出力に印字するだけ）。
  - よって Actions から流しても痕跡ゼロ＝完全に戻せる（スクリプト/WFを消せば元通り）。

これは「実データを見てから指標とパラメータを確定する」ための診断であって、本番反映ではない。
出力（ログ）を開発(Claude)に共有 → 仮値を確定 → 次段(B1/C2)へ。

パラメータ（env で上書き可・既定は暫定）：
  WINDOW_DAYS_REF=28  基準窓（7の倍数＝曜日均等）
  GAP_DAYS=1          直近を基準から除外（伸びの自己汚染防止）
  N0=10               収縮の強さ（点が薄いほどスコアを1へ寄せる）
  Z_MIN=2             有意性の下限（頑健z）。表示の参考に使う
  MIN_CURRENT=100     現在CCUの下限（仮）
  MIN_POINTS=7        基準窓内の観測点数の下限（仮）
  TOP_N=20            表示件数
"""

import os
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
WINDOW_DAYS_REF = int(os.environ.get("WINDOW_DAYS_REF") or "28")
GAP_DAYS = int(os.environ.get("GAP_DAYS") or "1")
N0 = float(os.environ.get("N0") or "10")
Z_MIN = float(os.environ.get("Z_MIN") or "2")
MIN_CURRENT = int(os.environ.get("MIN_CURRENT") or "100")
MIN_POINTS = int(os.environ.get("MIN_POINTS") or "7")
TOP_N = int(os.environ.get("TOP_N") or "20")

P = {"gap_days": GAP_DAYS, "window_days": WINDOW_DAYS_REF,
     "n0": N0, "min_current": MIN_CURRENT, "min_points": MIN_POINTS}

# --- 1) 履歴の実量・鮮度（28日基準が埋まるか／現在が古くないか）---
Q_OVERVIEW = """
SELECT min(recorded_at) AS first_at, max(recorded_at) AS last_at,
       count(*) AS total_rows, count(DISTINCT appid) AS n_appids,
       count(*) FILTER (WHERE recorded_at >= now() - interval '24 hours') AS rows_24h,
       count(*) FILTER (
         WHERE recorded_at <  now() - make_interval(days => %(gap_days)s)
           AND recorded_at >= now() - make_interval(days => %(gap_days)s + %(window_days)s)
       ) AS rows_ref
FROM player_counts;
"""

# --- 2) 基準窓の点数分布・被覆（何件がランク可能か）---
Q_COVERAGE = """
WITH ref AS (
  SELECT appid, count(*) AS n_points
  FROM player_counts
  WHERE recorded_at <  now() - make_interval(days => %(gap_days)s)
    AND recorded_at >= now() - make_interval(days => %(gap_days)s + %(window_days)s)
  GROUP BY appid
)
SELECT count(*) AS appids_with_ref,
       count(*) FILTER (WHERE n_points >= %(min_points)s) AS appids_ge_minpoints,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY n_points) AS median_n_points,
       min(n_points) AS min_n, max(n_points) AS max_n
FROM ref;
"""

# --- 3) 曜日効果（週末/平日の比の中央値）。avg+FILTER で安全に ---
Q_DOW = """
WITH d AS (
  SELECT appid,
         avg(player_count) FILTER (WHERE extract(dow FROM recorded_at) IN (0,6)) AS wknd_avg,
         avg(player_count) FILTER (WHERE extract(dow FROM recorded_at) NOT IN (0,6)) AS wkdy_avg
  FROM player_counts
  WHERE recorded_at >= now() - make_interval(days => %(gap_days)s + %(window_days)s)
  GROUP BY appid
)
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY wknd_avg / NULLIF(wkdy_avg,0)) AS median_weekend_ratio,
       count(*) FILTER (WHERE wknd_avg IS NOT NULL AND wkdy_avg IS NOT NULL AND wkdy_avg > 0) AS n
FROM d;
"""

# --- 4) A1 本体：現在＝直近24h中央値（無ければ最新値）／基準＝除外つき7倍数窓 ---
Q_A1 = """
WITH cur24 AS (
  SELECT appid, percentile_cont(0.5) WITHIN GROUP (ORDER BY player_count) AS c24, count(*) AS n24
  FROM player_counts WHERE recorded_at >= now() - interval '24 hours' GROUP BY appid
),
curlatest AS (
  SELECT DISTINCT ON (appid) appid, player_count AS clatest, recorded_at AS last_at
  FROM player_counts ORDER BY appid, recorded_at DESC
),
ref AS (
  SELECT appid,
         percentile_cont(0.5)  WITHIN GROUP (ORDER BY player_count) AS baseline,
         percentile_cont(0.25) WITHIN GROUP (ORDER BY player_count) AS q1,
         percentile_cont(0.75) WITHIN GROUP (ORDER BY player_count) AS q3,
         count(*) AS n_points
  FROM player_counts
  WHERE recorded_at <  now() - make_interval(days => %(gap_days)s)
    AND recorded_at >= now() - make_interval(days => %(gap_days)s + %(window_days)s)
  GROUP BY appid
)
SELECT cl.appid, g.name,
       COALESCE(c24.c24, cl.clatest)                          AS current_ccu,
       (c24.c24 IS NOT NULL)                                  AS used_24h,
       cl.last_at, r.baseline, r.n_points, (r.q3 - r.q1)      AS iqr,
       (COALESCE(c24.c24, cl.clatest) - r.baseline)           AS delta,
       COALESCE(c24.c24, cl.clatest)::float / NULLIF(r.baseline,0) AS raw_ratio,
       1 + (r.n_points::float / (r.n_points + %(n0)s))
           * (COALESCE(c24.c24, cl.clatest)::float / NULLIF(r.baseline,0) - 1) AS shrunk_ratio,
       (COALESCE(c24.c24, cl.clatest) - r.baseline) / NULLIF(r.q3 - r.q1, 0)   AS robust_z
FROM curlatest cl
JOIN games g ON g.appid = cl.appid
LEFT JOIN cur24 c24 ON c24.appid = cl.appid
LEFT JOIN ref  r    ON r.appid  = cl.appid
WHERE COALESCE(c24.c24, cl.clatest) >= %(min_current)s
  AND r.n_points >= %(min_points)s;
"""


def _fmt(x, nd=2):
    if x is None:
        return "—"
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return str(x)


def _print_rows(rows, key_idx, title):
    print(f"\n── {title}（上位{min(TOP_N, len(rows))}件）──")
    print("  name                         現在     平常     n   IQR     倍率  収縮後  頑健z  24h")
    for r in sorted(rows, key=lambda x: (x[key_idx] is None, -(x[key_idx] or -1e18)))[:TOP_N]:
        appid, name, cur, used24, last_at, base, n, iqr, delta, raw, shr, z = r
        nm = (name or str(appid))[:26].ljust(26)
        print(f"  {nm} {str(cur).rjust(8)} {_fmt(base,0).rjust(8)} {str(n).rjust(3)} "
              f"{_fmt(iqr,0).rjust(7)} {_fmt(raw).rjust(6)} {_fmt(shr).rjust(6)} "
              f"{_fmt(z).rjust(5)} {'y' if used24 else 'n'}")


def main():
    print("=" * 72)
    print("view02 A1 診断（読み取り専用・印字のみ・本番非変更）")
    print(f"params: WINDOW_DAYS_REF={WINDOW_DAYS_REF} GAP_DAYS={GAP_DAYS} N0={N0} "
          f"Z_MIN={Z_MIN} MIN_CURRENT={MIN_CURRENT} MIN_POINTS={MIN_POINTS} TOP_N={TOP_N}")
    print("=" * 72)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)  # 物理的に書けないよう固定
        with conn.cursor() as cur:

            try:
                cur.execute(Q_OVERVIEW, P)
                first_at, last_at, total, n_appids, rows_24h, rows_ref = cur.fetchone()
                print("\n[1] 履歴の実量・鮮度")
                print(f"  観測期間: {first_at} 〜 {last_at}")
                print(f"  総行数={total}  appid数={n_appids}  直近24h行数={rows_24h}  基準窓内行数={rows_ref}")
                print("  ※直近24h行数が0なら現在は最新値フォールバック。last_at が数日前なら鮮度に注意(ロードマップA4)。")
            except Exception as e:
                print(f"⚠ [1] overview 失敗: {type(e).__name__}: {e}")

            try:
                cur.execute(Q_COVERAGE, P)
                with_ref, ge_min, med_n, min_n, max_n = cur.fetchone()
                print("\n[2] 基準窓の被覆（ランク可能件数）")
                print(f"  基準データありappid={with_ref}  MIN_POINTS({MIN_POINTS})以上={ge_min}  "
                      f"n_points中央値={_fmt(med_n,1)} (min={min_n}, max={max_n})")
                print("  ※ge_min が小さすぎると公開時のランキングが薄い→窓やMIN_POINTSの再調整が要る。")
            except Exception as e:
                print(f"⚠ [2] coverage 失敗: {type(e).__name__}: {e}")

            try:
                cur.execute(Q_DOW, P)
                wknd_ratio, n_dow = cur.fetchone()
                print("\n[3] 曜日効果（週末/平日の比・中央値）")
                print(f"  median(週末÷平日)={_fmt(wknd_ratio)}  対象appid={n_dow}")
                print("  ※1.0から大きく離れるほど曜日偏りが強い＝『週末だけ強い』をランキングが誤検出しやすい。")
            except Exception as e:
                print(f"⚠ [3] dow 失敗: {type(e).__name__}: {e}")

            try:
                cur.execute(Q_A1, P)
                rows = cur.fetchall()
                print(f"\n[4] A1 ランキング（しきい値通過 {len(rows)} 件）")
                if not rows:
                    print("  0件。MIN_CURRENT/MIN_POINTS を緩めるか、窓を短くして再診断。")
                else:
                    _print_rows(rows, 10, "収縮後スコア順（提案する並び）")   # shrunk_ratio
                    _print_rows(rows, 8, "生の倍率順（収縮なし＝ノイズ確認用）")  # raw_ratio
                    n_sig = sum(1 for r in rows if (r[11] or 0) >= Z_MIN)
                    print(f"\n  有意性: robust_z>={Z_MIN} を満たすのは {n_sig}/{len(rows)} 件")
                    print("  ※『収縮後』と『生の倍率』で上位が大きく違うほど、収縮が小型ノイズを抑えている証拠。")
            except Exception as e:
                print(f"⚠ [4] A1 失敗（PG構文/権限/索引の可能性・全文を共有してください）: {type(e).__name__}: {e}")
    finally:
        conn.close()

    print("\n" + "=" * 72)
    print("この出力（[1]〜[4]全文）を開発(Claude)に共有 → 仮パラメータを確定 → B1/C2 へ。")
    print("書き込み・ファイル変更は一切していません（read-only固定）。")


if __name__ == "__main__":
    main()
