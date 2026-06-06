"""
③ 4Gamer RSS 収集: 日本語ゲームメディアの「早期露出」の一次データを、見出し+リンク+日時で貯める。
取得口 = 4Gamer 公式RSS（RSS 1.0/RDF形式）。4Gamerは規約で営利サイトでのRSS利用を明示許諾（本文/画像の無断転載は不可・RSS範囲のみ・直接課金要因でないこと）。
著作権/規約の安全側: 保存は「見出し(title)/リンク(url)/由来フィード(category)/日時(dc:date)」のメタのみ。description(本文要約)は保存しない。
重要: この段階では games との紐づけはしない（記事↔ゲームの対応付けは名寄せが誤リンク=誤情報を生むため、別問題として後日）。jp_news 単独で蓄積する。
重複排除: (source, guid=記事URL) ユニークに ON CONFLICT DO NOTHING（冪等）。
ガード(job_state): 直近 MIN_INTERVAL_HOURS 以内に成功してたらスキップ。低頻度で十分（告知より腐りにくい）。
失敗時: あるフィードが落ちても他フィードは続行（部分成功）。1件も取れなければ成功記録しない＝次回再試行。
"""
import os
import time
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import Counter

import psycopg2
from psycopg2.extras import execute_batch

DATABASE_URL = os.environ["DATABASE_URL"]

SOURCE = "4gamer"
BASE = "https://www.4gamer.net/rss/"
# 既定フィード（PC/Steam発見に効く一般＋ジャンル）。env FEEDS で "key=path,key=path" 形式の上書き可。
DEFAULT_FEEDS = {
    "news_topics": "news_topics.xml",   # 注目の記事
    "action":      "all_action.xml",
    "singlerpg":   "all_singlerpg.xml",
    "strategy":    "all_strategy.xml",
    "simulation":  "all_simulation.xml",
    "adventure":   "all_adventure.xml",
    "etc":         "all_etc.xml",
}
RATE_SLEEP = float(os.environ.get("JPNEWS_RATE_SLEEP") or "1.0")  # フィード間の間合い（礼儀）
JOB_NAME = "jp_news_4gamer"
MIN_INTERVAL_HOURS = int(os.environ.get("MIN_INTERVAL_HOURS") or "6")  # 告知より緩く・1日数回
FORCE = (os.environ.get("FORCE") or "").strip().lower() in ("1", "true", "yes")

NS_RSS = "{http://purl.org/rss/1.0/}"
NS_DC  = "{http://purl.org/dc/elements/1.1/}"
NS_RDF = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}"

_status = Counter()


def load_feeds():
    raw = (os.environ.get("FEEDS") or "").strip()
    if not raw:
        return dict(DEFAULT_FEEDS)
    feeds = {}
    for part in raw.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            feeds[k.strip()] = v.strip()
    return feeds or dict(DEFAULT_FEEDS)


def parse_dt(s):
    if not s:
        return None
    s = s.strip()
    for cand in (s, s.replace("Z", "+00:00")):
        try:
            return datetime.fromisoformat(cand)
        except ValueError:
            continue
    return None


def fetch_feed(path):
    """戻り値: 行のリスト [(source, guid, title, url, category, published_at), ...]。失敗時は None。"""
    url = BASE + path
    req = urllib.request.Request(url, headers={
        "User-Agent": "game-site-jpnews/0.1 (non-commercial-feedreader; RSS-range use)"
    })
    for _ in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            root = ET.fromstring(data)
            rows = []
            for item in root.iter(NS_RSS + "item"):
                guid = item.get(NS_RDF + "about") or item.findtext(NS_RSS + "link")
                if not guid:
                    continue
                title = (item.findtext(NS_RSS + "title") or "").strip()[:500]
                link = (item.findtext(NS_RSS + "link") or guid).strip()[:1000]
                pub = parse_dt(item.findtext(NS_DC + "date"))
                rows.append((SOURCE, guid.strip()[:1000], title, link, None, pub))
            _status["ok"] += 1
            return rows
        except urllib.error.HTTPError as e:
            if e.code == 429:
                _status["429"] += 1
                time.sleep(30)
                continue
            _status["http_%d" % e.code] += 1
            return None
        except Exception:
            _status["error"] += 1
            time.sleep(2)
    _status["giveup"] += 1
    return None


def should_run():
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


def main():
    if not should_run():
        return
    feeds = load_feeds()
    print(f"4Gamer RSS 収集開始: {len(feeds)} フィード {list(feeds.keys())}")

    all_rows = []
    got_any = False
    for i, (cat, path) in enumerate(feeds.items()):
        rows = fetch_feed(path)
        if rows is None:
            print(f"  - {cat} ({path}): 取得失敗（スキップ）")
        else:
            got_any = True
            # category を埋める（フィード由来）
            rows = [(s, g, t, u, cat, p) for (s, g, t, u, _c, p) in rows]
            all_rows.extend(rows)
            print(f"  - {cat} ({path}): {len(rows)} 件")
        if i < len(feeds) - 1:
            time.sleep(RATE_SLEEP)

    if not got_any:
        print("どのフィードも取得できませんでした。成功記録はしません（次回再試行）。")
        print("ステータス内訳:", dict(_status))
        return

    inserted_attempt = 0
    if all_rows:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn, conn.cursor() as cur:
                execute_batch(
                    cur,
                    "INSERT INTO jp_news (source, guid, title, url, category, published_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (source, guid) DO NOTHING",
                    all_rows, page_size=500,
                )
        finally:
            conn.close()
        inserted_attempt = len(all_rows)

    if all_rows:
        s = all_rows[0]
        print(f"  sample: title='{s[2][:50]}' url={s[3][:50]} published={s[5]}")
    print("ステータス内訳:", dict(_status))
    print(f"投入を試みた行: {inserted_attempt}（重複はスキップ）。")
    mark_success()


if __name__ == "__main__":
    main()
