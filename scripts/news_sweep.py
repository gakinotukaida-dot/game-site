"""
②告知: Steam 公式ニュース（ISteamNews/GetNewsForApp）を全名簿ローリングで取得し、announcements に保存する。
目的: 「大型アップデート/新DLC/イベント/事前登録」等を“理由”として出すための、配給テンポの一次データ。
取得口 = https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/（公式・APIキー不要＝CCUのキー枠を消費しない）。
著作権の安全側: maxlength=1 で本文を取りに行かず、保存は「見出し/URL/ラベル(feedlabel)/発信元(feedname)/日時」のメタのみ（本文・画像は保存しない）。
重複排除: (appid, gid) ユニークに ON CONFLICT DO NOTHING（冪等＝同じ記事を二重に入れない）。
対象 = ハイブリッド: 監視(watchlist)＋発売前(coming_soon)＋活動中を先に観て、残り枠で last_news_check_at 古い順に広げる。
失敗時の扱い:
  - ネットワーク/429 → last_news_check_at を進めない＝次回再試行（appdetails と同設計）。
  - 正常応答（ニュース0件含む） → last_news_check_at を進める（確認済み）。
取りこぼし対策ガード(job_state): 直近 MIN_INTERVAL_HOURS 以内に成功してたらスキップ。
長時間対策: FLUSH_EVERY 件ごとに逐次保存。
"""
import os
import json
import time
import threading
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import execute_batch

DATABASE_URL = os.environ["DATABASE_URL"]

NEWS_CAP     = int(os.environ.get("NEWS_CAP")     or "2000")   # 1回の観測上限（テストは 50 など）
ACTIVE_DAYS  = int(os.environ.get("ACTIVE_DAYS")  or "30")     # 「活動中」の直近日数（他ジョブと揃える）
RATE_PER_SEC = float(os.environ.get("NEWS_RATE")  or "5")      # api.steampowered.com は比較的寛容。安全側で控えめ
WORKERS      = int(os.environ.get("NEWS_WORKERS") or "4")
NEWS_COUNT   = int(os.environ.get("NEWS_COUNT")   or "10")     # 1アプリあたり取得する最新件数
FLUSH_EVERY  = int(os.environ.get("NEWS_FLUSH")   or "500")    # この件数（アプリ数）ごとに逐次保存
SAMPLE_LOG   = int(os.environ.get("NEWS_SAMPLE_LOG") or "3")

JOB_NAME = "news_sweep"
MIN_INTERVAL_HOURS = int(os.environ.get("MIN_INTERVAL_HOURS") or "20")
FORCE = (os.environ.get("FORCE") or "").strip().lower() in ("1", "true", "yes")

API = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"

_lock = threading.Lock()
_next = [0.0]
_interval = 1.0 / RATE_PER_SEC

_status = Counter()
_status_lock = threading.Lock()


def _throttle():
    with _lock:
        now = time.monotonic()
        if _next[0] > now:
            time.sleep(_next[0] - now)
            now = time.monotonic()
        _next[0] = now + _interval


def _note(code):
    with _status_lock:
        _status[code] += 1


def fetch_news(appid):
    """戻り値: (appid, "ok", [行...]) または (appid, "fail", None)。
    行 = (appid, gid, title, url, feedlabel, feedname, published_at) ※INSERTのプレースホルダ順。本文は取得・保存しない。"""
    _throttle()
    url = API + "?" + urllib.parse.urlencode(
        {"appid": appid, "count": NEWS_COUNT, "maxlength": 1, "format": "json"}
    )
    req = urllib.request.Request(url, headers={"User-Agent": "game-site-news/0.1"})
    for _ in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.load(r)
            appnews = (data or {}).get("appnews") or {}
            items = appnews.get("newsitems") or []
            rows = []
            for it in items:
                if not isinstance(it, dict):
                    continue
                gid = it.get("gid")
                if not gid:
                    continue
                ts = it.get("date")
                pub = None
                if isinstance(ts, (int, float)):
                    try:
                        pub = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    except (ValueError, OverflowError, OSError):
                        pub = None
                rows.append((
                    appid,
                    str(gid),
                    (it.get("title") or "")[:500],
                    (it.get("url") or "")[:1000],
                    (it.get("feedlabel") or "")[:200],
                    (it.get("feedname") or "")[:200],
                    pub,
                ))
            _note("ok")
            return appid, "ok", rows
        except urllib.error.HTTPError as e:
            if e.code == 429:
                _note("429")
                time.sleep(min(int(e.headers.get("Retry-After", "60") or "60"), 120))
                continue
            _note("http_%d" % e.code)
            return appid, "fail", None
        except Exception:
            _note("error")
            time.sleep(2)
    _note("giveup")
    return appid, "fail", None


def should_run():
    """直近 MIN_INTERVAL_HOURS 時間以内に成功していれば False。FORCEで無視。確認不能時は安全側でTrue（fail-open）。"""
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
    """正常完了を job_state に記録する（重複しても非破壊）。"""
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


def get_targets():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT appid FROM games "
                "ORDER BY (CASE WHEN status = 'watchlist' "
                "               OR coming_soon IS TRUE "
                "               OR (last_active_at IS NOT NULL "
                "                   AND last_active_at >= now() - (%s * interval '1 day')) "
                "          THEN 0 ELSE 1 END) ASC, "
                "         last_news_check_at ASC NULLS FIRST "
                "LIMIT %s",
                (ACTIVE_DAYS, NEWS_CAP),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def flush(buffer):
    """buffer = [(appid, status, rows), ...] を1回ぶんDBへ書き、(保存試行した記事数, 確認したアプリ数) を返す。"""
    news_rows = []
    checked = []
    for appid, status, rows in buffer:
        if status == "ok":
            checked.append(appid)
            if rows:
                news_rows.extend(rows)
        # "fail" は何もしない（last_news_check_at を進めない＝次回再試行）
    if not news_rows and not checked:
        return 0, 0
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            if news_rows:
                execute_batch(
                    cur,
                    "INSERT INTO announcements "
                    "  (appid, gid, title, url, feedlabel, feedname, published_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (appid, gid) DO NOTHING",
                    news_rows, page_size=500,
                )
            if checked:
                cur.execute(
                    "UPDATE games SET last_news_check_at = now() WHERE appid = ANY(%s)",
                    (checked,),
                )
    finally:
        conn.close()
    return len(news_rows), len(checked)


def main():
    if not should_run():
        return
    targets = get_targets()
    print(f"今回ニュースを観測する対象: {len(targets)} 件 "
          f"(cap={NEWS_CAP}, rate={RATE_PER_SEC}/s, workers={WORKERS}, count={NEWS_COUNT}, flush={FLUSH_EVERY})")

    buffer = []
    items_total = 0
    checked_total = 0
    shown = 0
    counts = Counter()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for appid, status, rows in ex.map(fetch_news, targets):
            counts[status] += 1
            buffer.append((appid, status, rows))
            if status == "ok" and rows and shown < SAMPLE_LOG:
                r0 = rows[0]
                print(f"  sample appid={appid}: items={len(rows)} "
                      f"latest_title='{r0[2][:50]}' feedlabel='{r0[4]}' published={r0[6]}")
                shown += 1
            if len(buffer) >= FLUSH_EVERY:
                it, ck = flush(buffer)
                items_total += it
                checked_total += ck
                buffer = []
                print(f"  …逐次保存: 確認 {checked_total} 件 / 記事 {items_total} 行"
                      f"（ok={counts['ok']} fail={counts['fail']}）")
    if buffer:
        it, ck = flush(buffer)
        items_total += it
        checked_total += ck

    print(f"取得: ok(応答あり)={counts['ok']} / fail(再試行)={counts['fail']}")
    print("ステータス内訳:", dict(_status))
    print(f"確認（last_news_check_at 更新）: {checked_total} 件。投入を試みた記事行: {items_total}（重複はスキップ）。")
    mark_success()


if __name__ == "__main__":
    main()
