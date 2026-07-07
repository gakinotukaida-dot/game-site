"""
表示用エクスポート（候補3「新着で伸び」＝P2・全ゲーム対象）── 2026-07-06 / v1
================================================================
役割：Neon を「読むだけ」で、**発売から日が浅く（release_date 基準）いま自分比で伸びている**ゲームを
      全ゲームから拾い、表示の箱(radar_shell)が読む JSON を data/new_rising.json に書き出す。
      now_ccu と同じ行スキーマ（rows[].detail.stats/history/genres/release）＝箱は同じ描画で表示できる。

設計の線（export_now_ccu.py と同方針・確定事項と整合）：
- DBは読み取り専用（SELECT のみ）。書き込み・スキーマ変更なし＝(B)・非破壊。新規収集ゼロ。
- 「新着」＝ games.release_date が直近 LAUNCH_DAYS 日以内 かつ coming_soon=false（自データの若さで判定しない＝分析メモP2）。
- 「伸び」＝ 自己比 = 現在CCU / 平常値(中央値, 直近 WINDOW_DAYS 日)。順位は自己比 降順。
- 早期・低確信を明示（点が少ない新作はノイジー）。平常値の点数(n_points)を隠さず出す＝箱が「履歴が浅い」を表示。
- 著作物は載せない（画像・説明文は持たない）。ジャンル等の短い分類語と公式リンクの素(appid)のみ。
- fail-soft：stats 取得に失敗しても本体は止めない（stats 無しで継続＝可逆）。

戻し方：このファイル/ワークフローを消すだけ（data/new_rising.json が古くなる/消えるのみ・DB無変更）。
env：LAUNCH_DAYS / WINDOW_DAYS / BUCKET_SEC / MIN_CURRENT / MIN_POINTS / ACTIVE_DAYS / TOP_N / RISE_MIN。
"""

import json
import os
from datetime import datetime

import psycopg2

from _filters import not_adult

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/new_rising.json"

LAUNCH_DAYS = int(os.environ.get("LAUNCH_DAYS") or "14")     # 「新着」＝発売からこの日数以内（P2既定14日）
ACTIVE_DAYS = int(os.environ.get("ACTIVE_DAYS") or "3")      # 直近この日数に観測がある＝稼働中
MIN_CURRENT = int(os.environ.get("MIN_CURRENT") or "50")     # 現CCUの下限（小さい新作も拾うため低め）
MIN_POINTS  = int(os.environ.get("MIN_POINTS")  or "3")      # 平常値の最低点数（薄すぎる比は出さない）
RISE_MIN    = float(os.environ.get("RISE_MIN")  or "1.0")    # 自己比の下限（1.0=平常以上のみ。0で全部）
TOP_N       = int(os.environ.get("TOP_N")       or "60")     # 出力件数の上限

DEV_MAX = int(os.environ.get("DEV_MAX") or "5")
GENRE_MAX = int(os.environ.get("GENRE_MAX") or "7")
CATEGORY_MAX = int(os.environ.get("CATEGORY_MAX") or "7")
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS") or "14")     # 平常値(中央値)＆履歴の窓
BUCKET_SEC = int(os.environ.get("BUCKET_SEC") or "21600")    # 履歴のバケット幅（既定6h）

# 候補＝発売直後（LAUNCH_DAYS内）・coming_soon=false・直近に観測あり・現CCU>=MIN_CURRENT。
CAND_QUERY = """
WITH latest AS (
  SELECT DISTINCT ON (appid) appid, player_count, recorded_at
  FROM player_counts
  WHERE recorded_at >= now() - make_interval(days => %(active_days)s)
  ORDER BY appid, recorded_at DESC
)
SELECT l.appid, g.name, l.player_count AS ccu, l.recorded_at,
       g.developers, g.publishers, g.genres, g.categories, g.dlc,
       g.release_date, g.release_date_text, g.website
FROM latest l JOIN games g ON g.appid = l.appid
WHERE g.coming_soon IS NOT TRUE
  AND """ + not_adult("g") + """
  AND g.release_date IS NOT NULL
  AND g.release_date >= (now()::date - %(launch_days)s)
  AND l.player_count >= %(min_current)s
"""

# 平常値(中央値)・24hピーク・観測ピーク・履歴（export_now_ccu.py と同一）。
STATS_QUERY = """
WITH win AS (
  SELECT appid, player_count, recorded_at
  FROM player_counts
  WHERE appid = ANY(%(appids)s)
    AND recorded_at >= now() - make_interval(days => %(window_days)s)
),
base AS (
  SELECT appid,
         percentile_cont(0.5) WITHIN GROUP (ORDER BY player_count) AS baseline,
         max(player_count) FILTER (WHERE recorded_at >= now() - interval '24 hours') AS peak24h,
         count(*) AS n_points
  FROM win GROUP BY appid
),
peakall AS (
  SELECT appid, max(player_count) AS peak_observed, count(*) AS n_total
  FROM player_counts WHERE appid = ANY(%(appids)s) GROUP BY appid
),
hist AS (
  SELECT appid,
         (floor(extract(epoch FROM recorded_at) / %(bucket)s) * %(bucket)s)::bigint AS ts,
         max(player_count) AS c
  FROM win GROUP BY appid, ts
)
SELECT pa.appid, b.baseline, b.peak24h, b.n_points, pa.peak_observed, pa.n_total,
       COALESCE((SELECT json_agg(json_build_array(h.ts, h.c) ORDER BY h.ts)
                 FROM hist h WHERE h.appid = pa.appid), '[]'::json) AS history
FROM peakall pa LEFT JOIN base b ON b.appid = pa.appid;
"""


def _as_list(cell):
    if cell is None:
        return []
    if isinstance(cell, (list, tuple)):
        return list(cell)
    if isinstance(cell, str):
        return json.loads(cell)
    return list(cell)


def _names(arr, cap):
    if not isinstance(arr, list):
        return []
    out = [str(x).strip() for x in arr if x and str(x).strip()]
    return out[:cap]


def _descs(arr, cap):
    if not isinstance(arr, list):
        return []
    out = []
    for x in arr:
        if isinstance(x, dict):
            d = x.get("description")
            if d and str(d).strip():
                out.append(str(d).strip())
        if len(out) >= cap:
            break
    return out


def _fetch_stats(cur, appids):
    if not appids:
        return {}
    cur.execute(STATS_QUERY, {"appids": list(appids), "window_days": WINDOW_DAYS, "bucket": BUCKET_SEC})
    out = {}
    for r in cur.fetchall():
        appid, baseline, peak24h, n_points, peak_observed, n_total, history = r
        out[appid] = {
            "stats": {
                "baseline": int(round(baseline)) if baseline is not None else None,
                "peak24h": int(peak24h) if peak24h is not None else None,
                "peak_observed": int(peak_observed) if peak_observed is not None else None,
                "n_points": int(n_points) if n_points is not None else 0,
            },
            "history": [[int(p[0]), int(p[1])] for p in _as_list(history)],
        }
    return out


def _release_iso(it):
    rd = it.get("release_date")
    if rd is None:
        return it.get("release_date_text")
    try:
        return rd.isoformat()
    except AttributeError:
        return str(rd)


def _build_detail(it):
    dlc = it.get("dlc")
    dlc_count = len(dlc) if isinstance(dlc, list) else 0
    st = it.get("_stats") or {}
    return {
        "developers": _names(it.get("developers"), DEV_MAX),
        "publishers": _names(it.get("publishers"), DEV_MAX),
        "genres": _descs(it.get("genres"), GENRE_MAX),
        "categories": _descs(it.get("categories"), CATEGORY_MAX),
        "dlc_count": dlc_count,
        "release": _release_iso(it),
        "website": it.get("website"),
        "siblings": [],
        "stats": {
            "baseline": st.get("baseline"),
            "peak24h": st.get("peak24h"),
            "peak_observed": st.get("peak_observed"),
            "n_points": st.get("n_points", 0),
        },
        "history": it.get("_history") or [],
    }


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)  # 物理的に書けないよう固定
        with conn.cursor() as cur:
            cur.execute(CAND_QUERY, {"active_days": ACTIVE_DAYS, "launch_days": LAUNCH_DAYS,
                                     "min_current": MIN_CURRENT})
            cols = [c[0] for c in cur.description]
            items = [dict(zip(cols, r)) for r in cur.fetchall()]
            appids = [it["appid"] for it in items]
            try:
                stats_by = _fetch_stats(cur, appids)
            except Exception as e:
                stats_by = {}
                print(f"⚠ stats/history 取得に失敗（stats無しで継続・可逆）: {type(e).__name__}: {e}")
            for it in items:
                s = stats_by.get(it["appid"], {})
                it["_stats"] = s.get("stats") or {}
                it["_history"] = s.get("history") or []
    finally:
        conn.close()

    # 自己比＝現在/平常値。平常値が薄い(n_points<MIN_POINTS)/無い ものは出さない（誤比を避ける＝低確信は明示、無比は捨てる）。
    rows = []
    for it in items:
        st = it.get("_stats") or {}
        bl = st.get("baseline")
        npts = st.get("n_points") or 0
        if bl is None or bl <= 0 or npts < MIN_POINTS:
            continue
        ratio = it["ccu"] / bl
        if ratio < RISE_MIN:
            continue
        it["_ratio"] = ratio
        rows.append(it)
    rows.sort(key=lambda it: it["_ratio"], reverse=True)
    rows = rows[:TOP_N]

    observed = None
    for it in items:
        raw = it.get("recorded_at")
        if raw is None:
            continue
        try:
            observed = raw.isoformat()
        except AttributeError:
            observed = str(raw)
        break

    out_rows = [
        {"appid": it["appid"], "name": it.get("name"), "ccu": it["ccu"],
         "ratio": round(it["_ratio"], 2), "detail": _build_detail(it)}
        for it in rows
    ]
    payload = {
        "view": "new_rising",
        "source": "steam_official_ccu + release_date",
        "schema": "v5-hist",
        "generated_at": datetime.now().astimezone().isoformat(),
        "params": {"launch_days": LAUNCH_DAYS, "window_days": WINDOW_DAYS,
                   "min_current": MIN_CURRENT, "min_points": MIN_POINTS, "rise_min": RISE_MIN},
        "count": len(out_rows),
        "rows": out_rows,
    }

    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")

    print(f"書き出し: {OUT_PATH}（{len(out_rows)} 件・発売{LAUNCH_DAYS}日内×自己比≥{RISE_MIN}・自己比降順）")
    for r in out_rows[:8]:
        d = r["detail"]
        print(f"  {r['appid']} {(r['name'] or '')[:24]:<24} 現在{r['ccu']:>7} 平常{d['stats']['baseline']} "
              f"×{r['ratio']} 発売{d['release']} 履歴{len(d['history'])}点(n={d['stats']['n_points']})")
    if not out_rows:
        print("該当0件＝いま『発売直後で自分比が伸びている』作品が無い（正常）。LAUNCH_DAYS/RISE_MIN を緩めて再確認可。")


if __name__ == "__main__":
    main()
