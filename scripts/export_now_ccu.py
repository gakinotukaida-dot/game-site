"""
表示用エクスポート（候補1「今のプレイヤー数」＋ L1詳細 ＋ L2同じ開発元の他作品 ＋ L3公式website）── 2026-06-06 / v4
================================================================
役割：Neon を「読むだけ」で各appidの最新CCU上位N件を取り、表示の箱(radar_shell)が読む
      JSON を data/now_ccu.json に書き出す。
  v2: 各行に L1 詳細(detail)＝開発元/販売元/ジャンル/カテゴリ/DLC有無/発売日。
  v3: detail に L2 siblings＝同じ開発元の他作品（自分除く・最新CCU降順・最大8）。
  v4: detail に L3 website＝公式website（games.website・null可。深掘りの「公式へ降りる」）。

設計の線（土台・確定事項・drilldown設計v1 と整合）：
- DBは読み取り専用（SELECT のみ）。書き込み・スキーマ変更は一切しない＝(B)・非破壊。
- L1/L2 とも **新規収集ゼロ**＝既に games に入っている列だけで作る。
- siblings は `developers ?| ARRAY[...]`（JSONB配列の要素一致）で導出。
  名寄せは **完全一致のbest-effort**＝表記ゆれは取りこぼし／別社の同名混在があり得る（箱で注記）。
  速度のため GIN 索引 `idx_games_developers_gin`（sql/l2_developers_index.sql）を推奨（無くても動くが遅い）。
- detail/siblings は箱の「詳しく」開示でのみ使う＝既定は出さない（段階的開示・根幹9）。
- 後方互換：行の {appid,name,ccu} は不変＝旧箱は detail/siblings を無視して動く。

戻し方：このファイルを v2/v1 に戻せば siblings/detail が消えるだけ（DB無変更・可逆）。
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
SIBLINGS_MAX = int(os.environ.get("SIBLINGS_MAX") or "8")   # §7既定：6〜8件。暫定・env可変。

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

# L2：同じ開発元の他作品（自分除く・最新CCU降順・最大N）。developers は JSONB 文字列配列。
SIBLING_QUERY = """
SELECT g2.appid, g2.name,
       (SELECT pc.player_count FROM player_counts pc
        WHERE pc.appid = g2.appid ORDER BY pc.recorded_at DESC LIMIT 1) AS ccu
FROM games g2
WHERE g2.developers ?| %s AND g2.appid <> %s
ORDER BY ccu DESC NULLS LAST
LIMIT %s
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
    """同じ開発元の他作品を返す。開発元名が無ければ []（突合しない）。"""
    names = _names(developers, 50)  # 突合に使う開発元名（複数社対応・上限ゆるめ）
    if not names:
        return []
    cur.execute(SIBLING_QUERY, (names, self_appid, SIBLINGS_MAX))
    return [{"appid": r[0], "name": r[1], "ccu": r[2]} for r in cur.fetchall()]


def _build_detail(it):
    dlc = it.get("dlc")
    dlc_count = len(dlc) if isinstance(dlc, list) else 0
    return {
        "developers": _names(it.get("developers"), DEV_MAX),
        "publishers": _names(it.get("publishers"), DEV_MAX),
        "genres": _descs(it.get("genres"), GENRE_MAX),
        "categories": _descs(it.get("categories"), CATEGORY_MAX),
        "dlc_count": dlc_count,
        "release": it.get("release_date") or it.get("release_date_text"),
        "website": it.get("website"),            # L3: 公式website（無ければ None＝箱はストアリンクのみ）
        "siblings": it.get("_siblings") or [],   # L2（_fetch_siblings で付与済み）
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
            # L2：各行の siblings を同じ読み取り接続で取得（開発元がある行のみ）。
            for it in items:
                it["_siblings"] = _fetch_siblings(cur, it.get("developers"), it.get("appid"))
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
        "schema": "v4-l3",   # v1=detailなし / v2-l1 / v3-l2(+siblings) / v4-l3(+website)
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

    print(f"書き出し: {OUT_PATH}（{len(rows)} 件・観測時刻 {observed_at}・schema v3-l2）")
    for r in rows[:5]:
        d = r["detail"]
        print(f"  sample appid={r['appid']} {(r['name'] or '')[:22]}: {r['ccu']}人 "
              f"dev={d['developers']} genres={d['genres'][:2]} dlc={d['dlc_count']} "
              f"siblings={[s['name'] for s in d['siblings'][:3]]}")
    if not rows:
        print("注意: 行が0件でした（player_counts がまだ空か、観測直後の可能性）。")
    else:
        miss = sum(1 for r in rows if not r["detail"]["developers"] and not r["detail"]["genres"])
        sib = sum(1 for r in rows if r["detail"]["siblings"])
        print(f"L1メタ未充足の行: {miss}/{len(rows)}（appdetails未到達＝ローリングで充足）／"
              f"L2 他作品が1件以上ついた行: {sib}/{len(rows)}")


if __name__ == "__main__":
    main()
