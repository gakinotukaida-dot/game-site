import os
import json
import time
import threading
import urllib.parse
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import execute_values

STEAM_API_KEY = os.environ["STEAM_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]

DAILY_CAP = int(os.environ.get("SWEEP_CAP") or "80000")  # 1回の観測上限（Steam約10万/日に余裕）
FLOOR = 1            # この同接以上を保存（0人の死亡ゲームは記録しない）
RATE_PER_SEC = 15    # 毎秒リクエスト上限（429回避の安全側）
WORKERS = 12
DORMANT_DAYS = 180   # 長期に床未満かつ無反応なら dormant へ
CCU_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

# ── 取りこぼし対策ガード（job_state）の設定 ──────────────────────────
# GitHubの無料スケジュールは遅延・取りこぼしが多い。対策としてこのジョブは
# 1日に複数回起動を試み、「直近 MIN_INTERVAL_HOURS 時間以内に成功していれば
# 即終了」することで、重い巡回を1日1回だけに保つ（job_state テーブルが必要）。
JOB_NAME = "daily_sweep"
MIN_INTERVAL_HOURS = int(os.environ.get("MIN_INTERVAL_HOURS") or "20")  # 暫定値（環境変数で調整可）
FORCE = (os.environ.get("FORCE") or "").strip().lower() in ("1", "true", "yes")  # 手動で強制実行

_lock = threading.Lock()
_next = [0.0]
_interval = 1.0 / RATE_PER_SEC


def _throttle():
    with _lock:
        now = time.monotonic()
        if _next[0] > now:
            time.sleep(_next[0] - now)
            now = time.monotonic()
        _next[0] = now + _interval


def fetch_ccu(appid):
    _throttle()
    url = CCU_URL + "?" + urllib.parse.urlencode({"appid": appid, "key": STEAM_API_KEY})
    for _ in range(4):
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                pc = json.load(r).get("response", {}).get("player_count")
            return appid, (pc if isinstance(pc, int) else None)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(min(int(e.headers.get("Retry-After", "60") or "60"), 120))
                continue
            return appid, None
        except Exception:
            time.sleep(2)
    return appid, None


def get_targets():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT appid FROM games WHERE status <> 'dormant' "
                "ORDER BY last_checked_at ASC NULLS FIRST LIMIT %s",
                (DAILY_CAP,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def write_results(checked, above):
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            if above:
                execute_values(
                    cur,
                    "INSERT INTO player_counts (appid, player_count) VALUES %s",
                    above, page_size=2000,
                )
            if checked:
                execute_values(
                    cur,
                    "UPDATE games AS g SET last_checked_at = now() "
                    "FROM (VALUES %s) AS v(appid) WHERE g.appid = v.appid",
                    [(a,) for a in checked], template="(%s)", page_size=5000,
                )
            if above:
                execute_values(
                    cur,
                    "UPDATE games AS g SET last_active_at = now(), "
                    "status = CASE WHEN g.status = 'dormant' THEN 'active' ELSE g.status END "
                    "FROM (VALUES %s) AS v(appid) WHERE g.appid = v.appid",
                    [(a,) for (a, _pc) in above], template="(%s)", page_size=5000,
                )
            cur.execute(
                "UPDATE games SET status = 'dormant' "
                "WHERE status = 'active' AND ever_popped = false "
                "AND last_active_at IS NULL AND first_seen < now() - (%s * interval '1 day')",
                (DORMANT_DAYS,),
            )
            demoted = cur.rowcount
        return demoted
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
    print(f"今回観測する対象: {len(targets)} 件")
    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for appid, pc in ex.map(fetch_ccu, targets):
            results.append((appid, pc))
    checked = [a for (a, pc) in results if pc is not None]
    above = [(a, pc) for (a, pc) in results if pc is not None and pc >= FLOOR]
    print(f"取得成功: {len(checked)} 件 / 保存(同接{FLOOR}以上): {len(above)} 件")
    demoted = write_results(checked, above)
    print(f"dormant へ格下げ: {demoted} 件")
    mark_success()


if __name__ == "__main__":
    main()
