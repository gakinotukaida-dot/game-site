"""
フェーズ2: レビューの定点観測（全名簿ハイブリッド・ローリング）

対象 = 監視リスト ∪「活動中(直近 ACTIVE_DAYS 日に同接>=1)」を先に → 残り枠で
全名簿へ last_review_check_at 古い順に広げる（案D本丸＝低CCU高販売を取りこぼさない）。
1ゲーム=1コールで store の appreviews を叩き、query_summary の総数だけを
review_snapshots に保存する（レビュー本文は取得しない）。

メモ:
- 取得口は store.steampowered.com（ストアフロント）。CCU巡回が使う Web API
  キー(STEAM_API_KEY)は不要・送らない＝CCUの10万/日コール枠とは別系統。
- レート上限は Valve 非公表・IP単位。GitHub Actions は共有IPなので控えめに。
  安全策: スロットル + 429バックオフ + cap で1回の件数を上限化（daily_sweep と同じ作り）。
- 失敗したゲームは last_review_check_at を進めない＝次回再試行（daily_sweep と同様）。
  全名簿ローリングなので、失敗分は次回また「古い順」で拾い直される（取りこぼさない）。
"""
import os
import json
import time
import threading
import urllib.parse
import urllib.request
import urllib.error
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ["DATABASE_URL"]

REVIEW_CAP    = int(os.environ.get("REVIEW_CAP")        or "30000")  # 1回の観測上限
ACTIVE_DAYS   = int(os.environ.get("ACTIVE_DAYS")       or "30")     # 「活動中」の直近日数（仮置き・データで較正）
RATE_PER_SEC  = int(os.environ.get("REVIEW_RATE")       or "8")      # store は控えめに（IP単位・非公表）
WORKERS       = int(os.environ.get("REVIEW_WORKERS")    or "8")
SAMPLE_LOG    = int(os.environ.get("REVIEW_SAMPLE_LOG") or "3")      # 先頭N件の生サマリをログ（検証用）

# ── 取りこぼし対策ガード（job_state）の設定 ──────────────────────────
# GitHubの無料スケジュールは遅延・取りこぼしが多い。対策としてこのジョブは
# 1日に複数回起動を試み、「直近 MIN_INTERVAL_HOURS 時間以内に成功していれば
# 即終了」することで、重い巡回を1日1回だけに保つ（job_state テーブルが必要）。
JOB_NAME = "review_sweep"
MIN_INTERVAL_HOURS = int(os.environ.get("MIN_INTERVAL_HOURS") or "20")  # 暫定値（環境変数で調整可）
FORCE = (os.environ.get("FORCE") or "").strip().lower() in ("1", "true", "yes")  # 手動で強制実行

REVIEWS_URL = "https://store.steampowered.com/appreviews/"
# 「全体・全言語・全購入種別」の総数を取るためのパラメータ。
# filter / day_range は付けない（時間窓で総数が絞られるのを避ける）。
# ※この組み合わせが本当に "全期間の総数" を返すかは、cap小のテストの実レスポンスで確認する。
REVIEW_PARAMS = {
    "json": "1",
    "language": "all",
    "purchase_type": "all",
    "review_type": "all",
    "num_per_page": "1",   # 本文は要らないので最小化
}

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


def fetch_summary(appid):
    _throttle()
    url = REVIEWS_URL + str(appid) + "?" + urllib.parse.urlencode(REVIEW_PARAMS)
    req = urllib.request.Request(url, headers={"User-Agent": "game-site-review-sweep/0.1"})
    for _ in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.load(r)
            if data.get("success") != 1:
                _note("not_success")
                return appid, None
            s = data.get("query_summary") or {}
            tp, tn, tr = s.get("total_positive"), s.get("total_negative"), s.get("total_reviews")
            rs = s.get("review_score")
            if not all(isinstance(x, int) for x in (tp, tn, tr)):
                _note("no_summary")
                return appid, None
            _note("ok")
            return appid, (tp, tn, tr, rs if isinstance(rs, int) else None)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                _note("429")
                time.sleep(min(int(e.headers.get("Retry-After", "60") or "60"), 120))
                continue
            _note("http_%d" % e.code)
            return appid, None
        except Exception:
            _note("error")
            time.sleep(2)
    _note("giveup")
    return appid, None


def get_targets():
    """対象 = 全名簿のハイブリッド・ローリング。
    監視リスト ∪「直近 ACTIVE_DAYS 日に同接>=1 を観測した活動中ゲーム」を先に観て
    （＝頻度を確保）、残り枠を last_review_check_at 古い順に全名簿へ広げる
    （＝案D本丸＝低CCUでもよく売れた作品を取りこぼさない）。cap 件まで。"""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT appid FROM games "
                "ORDER BY (CASE WHEN status = 'watchlist' "
                "               OR (last_active_at IS NOT NULL "
                "                   AND last_active_at >= now() - (%s * interval '1 day')) "
                "          THEN 0 ELSE 1 END) ASC, "
                "         last_review_check_at ASC NULLS FIRST "
                "LIMIT %s",
                (ACTIVE_DAYS, REVIEW_CAP),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def write_results(rows):
    if not rows:
        return
    appids = [a for (a, _tp, _tn, _tr, _rs) in rows]
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO review_snapshots "
                "(appid, total_positive, total_negative, total_reviews, review_score) VALUES %s",
                rows, page_size=2000,
            )
            execute_values(
                cur,
                "UPDATE games AS g SET last_review_check_at = now() "
                "FROM (VALUES %s) AS v(appid) WHERE g.appid = v.appid",
                [(a,) for a in appids], template="(%s)", page_size=5000,
            )
    finally:
        conn.close()


def should_run():
    """直近 MIN_INTERVAL_HOURS 時間以内に成功していれば False（=今日はもう回した）。
    FORCE 指定時は常に True。状態を確認できないときは安全側で True（収集を取りこぼさない＝fail-open）。"""
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
    """正常完了を job_state に記録する（重複しても非破壊なので失敗しても安全）。"""
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
    targets = get_targets()
    print(f"今回レビューを観測する対象: {len(targets)} 件 "
          f"(cap={REVIEW_CAP}, active_days={ACTIVE_DAYS}, rate={RATE_PER_SEC}/s, workers={WORKERS})")

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for appid, summ in ex.map(fetch_summary, targets):
            results.append((appid, summ))

    rows = []
    for a, summ in results:
        if summ is not None:
            tp, tn, tr, rs = summ
            rows.append((a, tp, tn, tr, rs))
    print(f"取得成功・保存: {len(rows)} 件 / 失敗: {len(results) - len(rows)} 件")
    print("ステータス内訳:", dict(_status))

    shown = 0
    for a, summ in results:
        if summ is not None and shown < SAMPLE_LOG:
            tp, tn, tr, rs = summ
            print(f"  sample appid={a}: total_reviews={tr} (pos={tp}, neg={tn}, score={rs})")
            shown += 1

    if SAMPLE_LOG > 0:
        _, ref = fetch_summary(730)  # CS2 を既知の参照に（パラメータ検証用）
        if ref:
            tp, tn, tr, rs = ref
            print(f"[参照] CS2(730): total_reviews={tr} (pos={tp}, neg={tn}, score={rs}) "
                  f"← Steamストアの『全てのレビュー』件数と概ね一致するか目視確認")
        else:
            print("[参照] CS2(730) の取得に失敗 → パラメータ要再確認")

    write_results(rows)
    print("保存完了。")
    mark_success()


if __name__ == "__main__":
    main()
