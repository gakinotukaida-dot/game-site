"""
表示用エクスポート（候補1「今のプレイヤー数」＋ L1詳細 ＋ L2同じ開発元 ＋ L3公式website ＋ L4観測履歴）── 2026-06-07 / v5
================================================================
役割：Neon を「読むだけ」で各appidの最新CCU上位N件を取り、表示の箱(radar_shell)が読む
      JSON を data/now_ccu.json に書き出す。
  v2: 各行に L1 詳細(detail)＝開発元/販売元/ジャンル/カテゴリ/DLC有無/発売日。
  v3: detail に L2 siblings＝同じ開発元の他作品（自分除く・最新CCU降順・最大8）。
  v4: detail に L3 website＝公式website（games.website・null可）。
  v5: detail に「観測の履歴」＝stats（平常値=中央値 / 24hピーク / 観測ピーク）＋history（時系列・バケット化）。
      → 詳細ページの「自己比（平常値ライン）・現在/24hピーク/観測ピーク」チャートの燃料（drilldown設計 / #3）。

設計の線（土台・確定事項・drilldown設計v1 と整合）：
- DBは読み取り専用（SELECT のみ）。書き込み・スキーマ変更は一切しない＝(B)・非破壊。
- 新規収集ゼロ＝既に player_counts / games にある値だけで作る。
- 平常値＝**中央値（percentile_cont 0.5）over 直近 WINDOW_DAYS 日**。理由＝瞬間スパイクに強い・少点でも安定・誇張しない（暫定・env可変）。
- history は payload を抑えるため **BUCKET_SEC 秒バケットの max** にダウンサンプル（既定6h＝14日で最大~56点/件）。
- 履歴が浅い間は stats/history が薄い（システム稼働が新しい）＝箱側で「履歴が浅い」を正直に出す。
- 後方互換：行の {appid,name,ccu} と detail の既存キーは不変＝旧箱は stats/history を無視して動く。

戻し方：このファイルを v4 に戻せば stats/history が消えるだけ（DB無変更・可逆）。WINDOW_DAYS/BUCKET_SEC は env で調整。
"""

import json
import os
from datetime import datetime

import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/now_ccu.json"
TOP_N = int(os.environ.get("TOP_N") or "100")

DEV_MAX = int(os.environ.get("DEV_MAX") or "5")
GENRE_MAX = int(os.environ.get("GENRE_MAX") or "7")
CATEGORY_MAX = int(os.environ.get("CATEGORY_MAX") or "7")
SIBLINGS_MAX = int(os.environ.get("SIBLINGS_MAX") or "8")

# v5：観測履歴・平常値の窓とバケット幅（暫定・env可変）
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS") or "14")     # 平常値(中央値)＆履歴の窓
BUCKET_SEC = int(os.environ.get("BUCKET_SEC") or "21600")    # 履歴のバケット幅（既定6h）

QUERY = """
WITH latest AS (
  SELECT DISTINCT ON (appid) appid, player_count, recorded_at
  FROM player_counts
  ORDER BY appid, recorded_at DESC
)
SELECT json_agg(
         json_build_object(
           'appid', l.appid, 'name', g.name,
           'ccu', l.player_count, 'observed_at', l.recorded_at,
           'developers', g.developers, 'publishers', g.publishers,
           'genres', g.genres, 'categories', g.categories,
           'dlc', g.dlc,
           'release_date', g.release_date, 'release_date_text', g.release_date_text,
           'website', g.website
         ) ORDER BY l.player_count DESC
       ) AS now_list
FROM (SELECT * FROM latest ORDER BY player_count DESC LIMIT %s) l
JOIN games g ON g.appid = l.appid;
"""

SIBLING_QUERY = """
SELECT g2.appid, g2.name,
       (SELECT pc.player_count FROM player_counts pc
        WHERE pc.appid = g2.appid ORDER BY pc.recorded_at DESC LIMIT 1) AS ccu
FROM games g2
WHERE g2.developers ?| %s AND g2.appid <> %s
ORDER BY ccu DESC NULLS LAST
LIMIT %s
"""

# v5：top-N の appid 群について、平常値(中央値)・24hピーク・観測ピーク・履歴(バケット化max) を一括取得。
# 読み取りのみ。window は WINDOW_DAYS（平常値＆履歴）、観測ピークは全期間。
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
  FROM win
  GROUP BY appid, ts
)
SELECT pa.appid,
       b.baseline, b.peak24h, b.n_points,
       pa.peak_observed, pa.n_total,
       COALESCE((SELECT json_agg(json_build_array(h.ts, h.c) ORDER BY h.ts)
                 FROM hist h WHERE h.appid = pa.appid), '[]'::json) AS history
FROM peakall pa
LEFT JOIN base b ON b.appid = pa.appid;
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


def _fetch_siblings(cur, developers, self_appid):
    names = _names(developers, 50)
    if not names:
        return []
    cur.execute(SIBLING_QUERY, (names, self_appid, SIBLINGS_MAX))
    return [{"appid": r[0], "name": r[1], "ccu": r[2]} for r in cur.fetchall()]


def _fetch_stats(cur, appids):
    """top-N の appid 群の stats と history をまとめて取得（読み取りのみ）。
    返り値: {appid: {"stats": {...}, "history": [[ts,c],...]}}。失敗・空は空辞書。"""
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
            # history は [[ts(int), ccu(int)], ...]（json_agg→list か str の両対応）
            "history": [[int(p[0]), int(p[1])] for p in _as_list(history)],
        }
    return out


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
        "release": it.get("release_date") or it.get("release_date_text"),
        "website": it.get("website"),
        "siblings": it.get("_siblings") or [],
        # v5：観測の履歴（無い/浅い場合は None/空＝箱が「履歴が浅い」を出す）
        "stats": {
            "baseline": st.get("baseline"),
            "peak24h": st.get("peak24h"),
            "peak_observed": st.get("peak_observed"),
            "n_points": st.get("n_points", 0),
        },
        "history": it.get("_history") or [],
    }


def _max_observed_at(items):
    best = None
    for it in items:
        raw = it.get("observed_at")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw)
        except (ValueError, TypeError):
            continue
        if best is None or dt > best:
            best = dt
    return best.isoformat() if best else None


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:  # 読み取りのみ（commit しない）
            cur.execute(QUERY, (TOP_N,))
            items = _as_list(cur.fetchone()[0])
            for it in items:
                it["_siblings"] = _fetch_siblings(cur, it.get("developers"), it.get("appid"))
            # v5：stats / history を一括取得（失敗しても本体は止めない＝安全側・可逆）。
            # 実Postgresで新クエリ(PG固有構文)が弾かれた場合でも、ここで握って stats 無しで継続する。
            # → Run は成功・サイトは安全（箱は「履歴を準備中」表示）・原因はログに明示。
            try:
                stats_by = _fetch_stats(cur, [it.get("appid") for it in items])
            except Exception as e:  # 失敗を本体に波及させない（観測履歴は任意の追加情報）
                stats_by = {}
                print("⚠ stats/history の取得に失敗しました（PG構文/権限/索引などの可能性）:")
                print(f"    {type(e).__name__}: {e}")
                print("  → stats 無しで継続します（schema は v5-hist のまま・箱は『履歴を準備中』表示）。")
                print("  → このメッセージ全文を開発（Claude）に共有してください。STATS_QUERY を修正します。")
            for it in items:
                s = stats_by.get(it.get("appid"), {})
                it["_stats"] = s.get("stats") or {}
                it["_history"] = s.get("history") or []
    finally:
        conn.close()

    observed_at = _max_observed_at(items)
    rows = [
        {"appid": it["appid"], "name": it.get("name"), "ccu": it["ccu"], "detail": _build_detail(it)}
        for it in items
    ]

    payload = {
        "view": "now_ccu",
        "source": "steam_official_ccu",
        "schema": "v5-hist",   # v4-l3 に stats/history を追加（後方互換）
        "observed_at": observed_at,
        "count": len(rows),
        "rows": rows,
    }

    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")

    print(f"書き出し: {OUT_PATH}（{len(rows)} 件・観測時刻 {observed_at}・schema v5-hist・窓{WINDOW_DAYS}日/バケット{BUCKET_SEC}s）")
    for r in rows[:5]:
        d = r["detail"]
        st = d["stats"]
        print(f"  sample appid={r['appid']} {(r['name'] or '')[:22]}: 現在{r['ccu']} "
              f"平常{st['baseline']} 24h{st['peak24h']} 観測{st['peak_observed']} "
              f"履歴{len(d['history'])}点(n={st['n_points']})")
    if not rows:
        print("注意: 行が0件でした（player_counts がまだ空か、観測直後の可能性）。")
    else:
        thin = sum(1 for r in rows if (r["detail"]["stats"]["n_points"] or 0) < 2)
        print(f"履歴が浅い行(窓内<2点): {thin}/{len(rows)}（システム稼働が新しいほど多い＝時間で充足）")


if __name__ == "__main__":
    main()
