"""
Epic 0円配布監視（Free to Keep＝もらえば永久所有）を Epic公式の公開JSONだけで拾う。
確定事項ログ §J / 引継ぎ §0.0（2026-06-07#4）の「Epicは0円のみ採用」実装。Steam の free_promo_sweep.py の姉妹版。

取得口（公式・公開・キー不要・GET）：
  https://store-site-backend-static.ak.epicgames.com/freeGamesPromotions
  ＝Epicストアの「無料ゲーム」欄が使っている公開JSON。認証不要・クエリ無しでも返るが、地域指定（country/locale）を付ける。
  一次確認（2026-06-07・Epic拡張_規約安定性_下調べ_2026-06-07.md）：
   - これは“公式の文書化された開発者API”ではない（コミュニティ通称 "unofficial Web API"）＝**△扱い**。
   - per-game CCU は Epic に無い（自己比の伸びは作れない）。順位/カタログGraphQLは不安定＋規約未確定ゆえ**不採用**。
   - 本sweepは「無料配布(0円)のみ・低頻度」に限定。順位/カタログには踏み込まない。

何を“いま無料”と判定するか（防御的に）：
  data.Catalog.searchStore.elements[] の各要素について、
   - price.totalPrice.discountPrice == 0（いまの実売が0）かつ
   - promotions.promotionalOffers[].promotionalOffers[] の中に start<=now<=end の窓があり
     discountSetting.discountPercentage == 0（＝100%オフ・無料）であるもの。
  → その窓の endDate を promo_end（「いつまで」）に採る。startDate を promo_start に採る。
  upcomingPromotionalOffers（来週の無料予告）は v1 では拾わない（現在配布中のみ＝Steam側と揃える）。
  ※ JSON構造は変わり得るので .get と try で固める。欠けても落とさず skip（fail-soft）。
  ★検証済み（2026-06-07・実エンドポイントを web_fetch で取得して確認）：
   - Epic の discountSetting.discountPercentage は「**支払う割合**」＝**0 が無料**（例 Eternal Threads は 20＝80%オフで$3.99＝無料でない）。本コードの pct==0 判定はこの実挙動と一致。
   - promotions は **null** のことがある（catalogだけの行）／offerMappings は **空[]** のことがある（その場合 catalogNs.mappings→productSlug→urlSlug の順で slug を拾う）。
   - レスポンスは data と並んで errors[]（一部要素の404 等）を含むことがある＝**正常**。本コードは data だけ読み errors は無視（fail-soft）。
   - 実データ先頭群（freegamesカテゴリだが現在は有料の spotlight 行）で **誤検出ゼロ**を確認。本物の0円（pct=0・window が現在を含む）だけ抽出。

保存：epic_free_promos に upsert（namespace, offer_id を主キー）。
  promo_end/promo_start/page_slug/title は COALESCE で温存（後の取得が NULL で既存値を潰さない）。
取りこぼし対策ガード(job_state)：直近 MIN_INTERVAL_HOURS 以内に成功してたらスキップ（FORCE で無視）。fail-open。

(B)・根幹9・著作権の整理（不変）：
 - 表示するのは「自分で観測した“いまEpicで0円配布中”という事実＋公式ストアへのリンク」＝(B)自社表示の範囲。
   生データ（一覧・数値）の再提供はしない。煽りバナー・カウントダウン強調はしない（静かに＝根幹9）。
 - **Epic画像のホットリンク可否は未確認＝当面は画像を保存・表示しない（テキスト＋リンクのみ）**（確定事項§J）。
 - egdata.app 等の第三者は本番ソースにしない（参考のみ）。最終判断は弁護士（プロジェクト規律）。
"""
import os
import json
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_batch

DATABASE_URL = os.environ["DATABASE_URL"]

COUNTRY = os.environ.get("EPIC_FREE_COUNTRY") or "US"      # 無料配布は概ねグローバルだが地域を固定して安定化
LOCALE  = os.environ.get("EPIC_FREE_LOCALE")  or "en-US"
TIMEOUT = int(os.environ.get("EPIC_FREE_TIMEOUT") or "30")
RETRIES = int(os.environ.get("EPIC_FREE_RETRIES") or "4")

JOB_NAME = "epic_free_sweep"
MIN_INTERVAL_HOURS = int(os.environ.get("MIN_INTERVAL_HOURS") or "6")  # Epic無料は週替わり＝低頻度で十分（暫定）
FORCE = (os.environ.get("FORCE") or "").strip().lower() in ("1", "true", "yes")

BASE = "https://store-site-backend-static.ak.epicgames.com/freeGamesPromotions"
STORE_P = "https://store.epicgames.com/p/"   # + page_slug


def should_run():
    """直近 MIN_INTERVAL_HOURS 以内に成功していれば False（FORCE で無視）。確認不能時は安全側で True（fail-open）。"""
    if FORCE:
        print("[guard] FORCE 指定のためクールダウンを無視して実行します。")
        return True
    try:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT (last_success_at > now() - (%s || ' hours')::interval) "
                    "FROM job_state WHERE job = %s",
                    (MIN_INTERVAL_HOURS, JOB_NAME),
                )
                row = cur.fetchone()
        finally:
            conn.close()
        if row and row[0]:
            print(f"[guard] 直近 {MIN_INTERVAL_HOURS}h 以内に成功済み。今回はスキップ（FORCE=trueで強制実行可）。")
            return False
        return True
    except Exception as e:
        print(f"[guard] 判定不能のため安全側で実行します（fail-open）: {e}")
        return True


def record_success():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO job_state (job, last_success_at) VALUES (%s, now()) "
                    "ON CONFLICT (job) DO UPDATE SET last_success_at = EXCLUDED.last_success_at",
                    (JOB_NAME,),
                )
        finally:
            conn.close()
        print("[guard] 成功記録あり（job_state 更新）。")
    except Exception as e:
        print(f"[guard] 成功記録に失敗（致命的でない）: {e}")


def fetch_json():
    qs = urllib.parse.urlencode({"locale": LOCALE, "country": COUNTRY, "allowCountries": COUNTRY})
    url = f"{BASE}?{qs}"
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"Content-Type": "application/json",
                                                       "User-Agent": "game-site-radar/1.0 (+free-promo monitor)"})
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            print(f"[fetch] 試行 {attempt}/{RETRIES} 失敗: {e}")
    raise RuntimeError(f"freeGamesPromotions 取得に失敗: {last_err}")


def _parse_dt(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _page_slug(el):
    # 優先順に複数の場所を試す（構造差・欠損に強く）
    for m in (el.get("offerMappings") or []):
        if m.get("pageSlug"):
            return m["pageSlug"]
    cns = (el.get("catalogNs") or {}).get("mappings") or []
    for m in cns:
        if m.get("pageSlug"):
            return m["pageSlug"]
    return el.get("productSlug") or el.get("urlSlug") or None


def _active_free_window(el, now):
    """start<=now<=end かつ discountPercentage==0（=無料）の窓を返す。無ければ None。"""
    promos = (el.get("promotions") or {}).get("promotionalOffers") or []
    for group in promos:
        for off in (group.get("promotionalOffers") or []):
            start = _parse_dt(off.get("startDate"))
            end = _parse_dt(off.get("endDate"))
            pct = ((off.get("discountSetting") or {}).get("discountPercentage"))
            if start and start > now:
                continue
            if end and end < now:
                continue
            if pct == 0:   # Epic慣行：discountPercentage 0 = 実売0%（無料）
                return start, end
    return None


def extract_free(data, now):
    """[(namespace, offer_id, title, page_slug, promo_start, promo_end), ...]"""
    out = []
    try:
        elements = data["data"]["Catalog"]["searchStore"]["elements"]
    except Exception:
        print("[parse] 期待した構造が無い（data.Catalog.searchStore.elements）。0件で返す。")
        return out
    for el in elements:
        try:
            price = (((el.get("price") or {}).get("totalPrice") or {}).get("discountPrice"))
            if price not in (0, "0"):
                continue
            win = _active_free_window(el, now)
            if win is None:
                continue
            ns = el.get("namespace")
            oid = el.get("id")
            if not ns or not oid:
                continue
            out.append((ns, oid, el.get("title"), _page_slug(el), win[0], win[1]))
        except Exception as e:
            print(f"[parse] 1要素スキップ（fail-soft）: {e}")
            continue
    return out


def upsert(rows):
    """promo_end 等は COALESCE で温存（後の取得が NULL で既存値を潰さない）。"""
    if not rows:
        return 0
    with psycopg2.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            execute_batch(
                cur,
                "INSERT INTO epic_free_promos "
                "  (namespace, offer_id, kind, title, page_slug, promo_start, promo_end) "
                "VALUES (%s, %s, 'keep', %s, %s, %s, %s) "
                "ON CONFLICT (namespace, offer_id) DO UPDATE SET "
                "  title       = COALESCE(EXCLUDED.title,       epic_free_promos.title), "
                "  page_slug   = COALESCE(EXCLUDED.page_slug,   epic_free_promos.page_slug), "
                "  promo_start = COALESCE(EXCLUDED.promo_start, epic_free_promos.promo_start), "
                "  promo_end   = COALESCE(EXCLUDED.promo_end,   epic_free_promos.promo_end), "
                "  last_seen_at = now()",
                rows, page_size=200,
            )
    return len(rows)


def main():
    if not should_run():
        return
    now = datetime.now(timezone.utc)
    data = fetch_json()
    rows = extract_free(data, now)
    n = upsert(rows)
    record_success()
    titles = ", ".join(f"{r[2] or r[0]}" for r in rows) if rows else "(なし)"
    print(f"[done] いま無料配布中の Epic ゲーム: {n} 件 / {titles}")


if __name__ == "__main__":
    main()
