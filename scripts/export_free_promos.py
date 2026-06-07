"""
「無料でもらえる」横断レンズ用エクスポート（案A配信）。
Neon を読むだけで data/free_promos.json を生成する。export_now_ccu.py の姉妹版。

設計の正＝freepromos_box_design_v1_2026-06-07.md / 確定事項ログ §H・§J。
- DBは読み取り専用（SELECT のみ）。書き込み・スキーマ変更なし＝(B)・非破壊。STEAM_API_KEY 不要。
- 両方A：Steam と Epic を別配列で持つ（混ぜない）。箱はストア別ブロックで表示。
- v1はテキストのみ＝画像フィールドは出さない（将来 両ストア対称で追加）。
- 鮮度窓：Steam=12h（free-promo sweep は4h毎）／Epic=30h（epic_free sweep は1日2回）。表示SQLと揃える。
- 片方のテーブルが未作成でも落とさない（fail-soft・per-store）。配布ゼロは正常（空配列）。

出力スキーマ（free_promos_v1）：
{
  "generated_at": "...Z", "schema": "free_promos_v1",
  "counts": {"steam": N, "epic": M},
  "stores": {
    "steam": [{"title","ends_at"(ISO|null),"url"}],
    "epic":  [{"title","ends_at"(ISO|null),"url"(|null)}]
  }
}
"""
import os
import json
from datetime import datetime, timezone

import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("FREE_OUT_PATH") or "data/free_promos.json"

STEAM_SQL = """
SELECT g.name,
       fp.discount_end_date,
       'https://store.steampowered.com/app/' || fp.appid AS url
FROM free_promos fp
JOIN games g ON g.appid = fp.appid
WHERE fp.kind = 'keep'
  AND fp.last_seen_at > now() - interval '12 hours'
  AND (fp.discount_end_date IS NULL OR fp.discount_end_date > now())
ORDER BY (fp.discount_end_date IS NULL), fp.discount_end_date ASC NULLS LAST, fp.last_seen_at DESC
"""

EPIC_SQL = """
SELECT ep.title,
       ep.promo_end,
       CASE WHEN ep.page_slug IS NOT NULL
            THEN 'https://store.epicgames.com/p/' || ep.page_slug ELSE NULL END AS url
FROM epic_free_promos ep
WHERE ep.kind = 'keep'
  AND ep.last_seen_at > now() - interval '30 hours'
  AND (ep.promo_end IS NULL OR ep.promo_end > now())
ORDER BY (ep.promo_end IS NULL), ep.promo_end ASC NULLS LAST, ep.last_seen_at DESC
"""


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


def _fetch(cur, sql, store):
    """1ストア分を取得。テーブル未作成等は空で返す（fail-soft・他ストアを巻き込まない）。"""
    try:
        cur.execute(sql)
        out = []
        for name, ends, url in cur.fetchall():
            out.append({"title": name, "ends_at": _iso(ends), "url": url})
        return out
    except Exception as e:
        print(f"[{store}] 取得スキップ（テーブル未作成など・fail-soft）: {e}")
        return []


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        # 1ストアの失敗でトランザクションが汚れても他に波及しないよう、各クエリを独立カーソル＋rollbackで隔離
        steam = []
        epic = []
        with conn.cursor() as cur:
            steam = _fetch(cur, STEAM_SQL, "steam")
        conn.rollback()
        with conn.cursor() as cur:
            epic = _fetch(cur, EPIC_SQL, "epic")
        conn.rollback()
    finally:
        conn.close()

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "schema": "free_promos_v1",
        "counts": {"steam": len(steam), "epic": len(epic)},
        "stores": {"steam": steam, "epic": epic},
    }
    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
    print(f"書き出し: {OUT_PATH}（steam {len(steam)} / epic {len(epic)} 件・schema free_promos_v1）")


if __name__ == "__main__":
    main()
