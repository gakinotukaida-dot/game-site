"""
0円配布監視（Free to Keep＝100%オフ・もらえば永久所有）を公式情報だけで拾う。free weekend は v1 対象外。
2経路を併用して free_promos（現在状態テーブル）へ upsert する：

 (A) price_scan … 既存 price_snapshots の「各appidの最新値」が final=0 かつ discount_percent=100、
     かつ games.is_free が真でない（＝F2Pでない有料ゲームが100%オフ）行を拾う。DBのみ・Steam無アクセス。
     幅（appdetailsが触れた範囲）は広いが、終了日は分からず（NULL）、鮮度は appdetails ローリング次第。
 (B) featured … store.steampowered.com/api/featuredcategories を1コール叩き、掲載中の配布
     （final_price=0 かつ discount_percent=100）を拾う。**discount_expiration＝終了日が取れる**のが要点。
     掲載中の数本に限られる（網羅でない）が、終了日つき・高頻度で「生きてるうちに」拾える。

なぜこの2本立てか（一次情報の制約・2026-06-06調査）：
 - appdetails の price_overview は currency/initial/final/discount_percent のみ＝**終了日は入っていない**。
 - 終了日(discount_expiration)が公式・キー不要で取れる近道は featuredcategories だが「掲載中の数本」だけ。
 - よって「幅＝price_scan」「終了日と鮮度＝featured」を合わせ、終了日が無いものは表示側で「ストアで確認」に倒す。

取得口（featured）= https://store.steampowered.com/api/featuredcategories（ストアフロント・APIキー不要）。
  1コールで多数カテゴリ（specials 等）を返す軽い呼び出し。CCUのキー枠（GetNumberOfCurrentPlayers）とは別系統。
保存：free_promos に upsert（appid,kind を主キー）。featured を先に流して終了日をセットし、
  price_scan は終了日を **COALESCE で温存**（後から終了日を NULL で潰さない）。
ノイズ除去：featured には package/bundle 等 appid でない id も混じるため、games に存在する appid だけ採用。
取りこぼし対策ガード(job_state)：直近 MIN_INTERVAL_HOURS 以内に成功してたらスキップ（FORCE で無視）。fail-open。

(B)目的・根幹9・著作権の整理：
 - 表示するのは「自分で観測した“いま0円配布中”という事実＋公式ストアへのリンク」＝(B)自社表示の範囲。生データの再提供はしない。
 - 配布リストはユーザーが探している情報（見落とし拾い）＝ノイズでない（根幹9）。煽りバナー・カウントダウン強調はしない（静かに）。
 - featuredcategories は appdetails と同系統のストアフロントAPI（JSON・スクレイピングでない）。最終判断は弁護士（プロジェクト規律）。
"""
import os
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_batch

DATABASE_URL = os.environ["DATABASE_URL"]

CC      = os.environ.get("FREE_PROMO_CC")   or "jp"        # 価格地域（JPY）。0円判定自体は地域非依存だが揃える
LANG    = os.environ.get("FREE_PROMO_LANG") or "english"   # 表示名は games から引くので言語は判定に無関係
TIMEOUT = int(os.environ.get("FREE_PROMO_TIMEOUT") or "30")
RETRIES = int(os.environ.get("FREE_PROMO_RETRIES") or "4")

JOB_NAME = "free_promo_sweep"
MIN_INTERVAL_HOURS = int(os.environ.get("MIN_INTERVAL_HOURS") or "2")  # 暫定（featuredは1コールと軽い＝短めでよい）
FORCE = (os.environ.get("FORCE") or "").strip().lower() in ("1", "true", "yes")

FEATURED_API = "https://store.steampowered.com/api/featuredcategories"


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
                    "SELECT last_success_at, "
                    "       (last_success_at > now() - (%s * interval '1 hour')) AS too_soon "
                    "FROM job_state WHERE job = %s",
                    (MIN_INTERVAL_HOURS, JOB_NAME),
                )
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception as e:
        print(f"[guard] 状態の確認に失敗（{e}）。安全側で実行します（fail-open）。")
        return True
    if row and row[0] is not None and row[1]:
        print(f"[guard] 直近 {MIN_INTERVAL_HOURS}h 以内に成功済み（last_success_at={row[0]}）。今回はスキップします。")
        return False
    return True


def mark_success():
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
        print(f"[guard] 成功を記録しました（job={JOB_NAME}）。")
    except Exception as e:
        print(f"[guard] 成功の記録に失敗（{e}）。次回は再実行されます。")


def _to_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def fetch_featured():
    """featuredcategories を1コール叩き、(appid, discount_end_date|None) のリストを返す。
       Free to Keep の判定＝ final_price==0 かつ discount_percent==100（F2Pは discount_percent=0 なので除外）。"""
    url = FEATURED_API + "?" + urllib.parse.urlencode({"cc": CC, "l": LANG})
    req = urllib.request.Request(url, headers={"User-Agent": "game-site-free-promo/0.1"})
    data = None
    for _ in range(RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                data = json.load(r)
            break
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = min(int(e.headers.get("Retry-After", "60") or "60"), 120)
                print(f"[featured] 429。{wait}s 待機して再試行。")
                time.sleep(wait)
                continue
            print(f"[featured] HTTP {e.code}。featured はスキップ（price_scan は続行）。")
            return []
        except Exception as e:
            print(f"[featured] 取得エラー（{e}）。2s 後に再試行。")
            time.sleep(2)
    if not isinstance(data, dict):
        print("[featured] 応答が辞書でない＝スキップ。")
        return []

    found = {}  # appid -> discount_end_date(datetime|None)（重複は終了日が取れた方を優先）
    # カテゴリ名に依存せず、items を持つ全カテゴリを総なめ（specials/coming_soon/top_sellers/... 構造変化に強く）。
    for value in data.values():
        items = value.get("items") if isinstance(value, dict) else None
        if not isinstance(items, list):
            continue
        for it in items:
            if not isinstance(it, dict):
                continue
            appid = _to_int(it.get("id"))
            if appid is None:
                continue
            final_price = it.get("final_price")
            discount_percent = it.get("discount_percent")
            # Free to Keep のみ採用（final=0 かつ 100%オフ）。free weekend は v1 対象外。
            if final_price == 0 and discount_percent == 100:
                end = None
                ts = it.get("discount_expiration")
                if isinstance(ts, int) and ts > 0:
                    end = datetime.fromtimestamp(ts, tz=timezone.utc)
                # 同じappidが複数カテゴリに出たら終了日が取れた方を残す
                if appid not in found or (end is not None and found[appid] is None):
                    found[appid] = end
    print(f"[featured] Free to Keep 候補: {len(found)} 件（終了日つき {sum(1 for v in found.values() if v)} 件）")
    return [(aid, end) for aid, end in found.items()]


def fetch_price_scan():
    """price_snapshots の各appid最新値が final=0 & 100%オフ & 有料(is_free!=true) の appid を返す（DBのみ）。"""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "WITH latest AS ("
                "  SELECT DISTINCT ON (appid) appid, final, discount_percent "
                "  FROM price_snapshots "
                "  ORDER BY appid, recorded_at DESC"
                ") "
                "SELECT l.appid FROM latest l "
                "JOIN games g ON g.appid = l.appid "
                "WHERE l.final = 0 AND l.discount_percent = 100 "
                "  AND (g.is_free IS DISTINCT FROM TRUE)"
            )
            rows = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    print(f"[price_scan] 既存価格から Free to Keep 候補: {len(rows)} 件（終了日は不明＝NULL）")
    return rows


def upsert(rows):
    """rows = [(appid, source, discount_end_date|None), ...] を free_promos へ upsert。
       games に存在する appid のみ採用（featured の package/bundle 等のノイズを落とす）。
       discount_end_date は COALESCE で温存（後の price_scan が NULL で終了日を潰さない）。"""
    if not rows:
        return 0
    params = [(aid, "keep", src, end, aid) for (aid, src, end) in rows]
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            execute_batch(
                cur,
                "INSERT INTO free_promos (appid, kind, source, discount_end_date) "
                "SELECT %s, %s, %s, %s "
                "WHERE EXISTS (SELECT 1 FROM games g WHERE g.appid = %s) "
                "ON CONFLICT (appid, kind) DO UPDATE SET "
                "  source = EXCLUDED.source, "
                "  discount_end_date = COALESCE(EXCLUDED.discount_end_date, free_promos.discount_end_date), "
                "  last_seen_at = now()",
                params, page_size=200,
            )
    finally:
        conn.close()
    return len(params)


def main():
    if not should_run():
        return
    # featured を先（終了日をセット）→ price_scan（終了日は COALESCE で温存）。
    featured = fetch_featured()                 # [(appid, end|None), ...]
    scan = fetch_price_scan()                   # [appid, ...]
    rows = [(aid, "featured", end) for (aid, end) in featured]
    feat_ids = {aid for (aid, _) in featured}
    rows += [(aid, "price_scan", None) for aid in scan if aid not in feat_ids]

    n = upsert(rows)
    print(f"free_promos へ upsert 対象: {len(rows)} 件（featured {len(featured)} / price_scan単独 {len(rows)-len(featured)}）。"
          f"games存在チェック後にDB反映。")
    # 反映後の現在配布中（鮮度12h以内）の件数を軽く確認（任意）。
    try:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM free_promos "
                    "WHERE last_seen_at > now() - interval '12 hours' "
                    "  AND (discount_end_date IS NULL OR discount_end_date > now())"
                )
                print(f"現在配布中（last_seen 12h以内・未終了）: {cur.fetchone()[0]} 件")
        finally:
            conn.close()
    except Exception as e:
        print(f"（確認クエリは省略：{e}）")
    mark_success()


if __name__ == "__main__":
    main()
