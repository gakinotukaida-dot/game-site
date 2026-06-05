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

# 密ティア＝ status='watchlist' ∪「直近観測CCUの上位 DENSE_N 件」を毎回動的に算出する。
# “動き出した新顔”は広い daily_sweep が拾い、CCUが上がれば自動的にこの上位Nに入ってくる。
DENSE_N = int(os.environ.get("DENSE_N") or "300")            # 暫定。データを見て増減可。
FLOOR = int(os.environ.get("FLOOR") or "1")                  # この同接以上を保存（daily_sweep と同じ既定=1）
RATE_PER_SEC = int(os.environ.get("RATE_PER_SEC") or "12")   # 毎秒リクエスト上限（429回避の安全側）
WORKERS = int(os.environ.get("WORKERS") or "8")
CCU_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

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
    """密ティア＝ status='watchlist' ∪「直近観測CCUの上位 DENSE_N 件」（dormant 除外）。
    player_counts がまだ無い appid は上位Nに入らないが、watchlist は常に含める。"""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "WITH latest AS ("
                "  SELECT DISTINCT ON (appid) appid, player_count"
                "  FROM player_counts ORDER BY appid, recorded_at DESC"
                "), top_active AS ("
                "  SELECT l.appid FROM latest l JOIN games g ON g.appid = l.appid"
                "  WHERE g.status <> 'dormant'"
                "  ORDER BY l.player_count DESC LIMIT %s"
                ") "
                "SELECT appid FROM games WHERE status = 'watchlist' "
                "UNION SELECT appid FROM top_active",
                (DENSE_N,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def write_results(checked, above):
    """checked=取得できた全 appid（last_checked_at 更新）, above=同接 FLOOR 以上（player_counts 追加）。
    すべて INSERT / 時刻 UPDATE のみ＝非破壊。status は変えない（昇格・格下げは行わない）。"""
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
            active = [(a, pc) for (a, pc) in above if pc > 0]
            if active:
                execute_values(
                    cur,
                    "UPDATE games AS g SET last_active_at = now() "
                    "FROM (VALUES %s) AS v(appid) WHERE g.appid = v.appid",
                    [(a,) for (a, _pc) in active], template="(%s)", page_size=5000,
                )
    finally:
        conn.close()


def main():
    targets = get_targets()
    print(f"密ティア対象: {len(targets)} 件 "
          f"(DENSE_N={DENSE_N}, floor={FLOOR}, rate={RATE_PER_SEC}/s, workers={WORKERS})")
    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for appid, pc in ex.map(fetch_ccu, targets):
            results.append((appid, pc))
    checked = [a for (a, pc) in results if pc is not None]
    above = [(a, pc) for (a, pc) in results if pc is not None and pc >= FLOOR]
    fails = len(targets) - len(checked)
    print(f"取得成功: {len(checked)} 件 / 保存(同接{FLOOR}以上): {len(above)} 件 / 取得失敗: {fails} 件")
    write_results(checked, above)
    # 直近に保存した記録のサンプル（目視確認用）
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pc.appid, g.name, pc.player_count "
                "FROM player_counts pc JOIN games g ON g.appid = pc.appid "
                "ORDER BY pc.recorded_at DESC, pc.player_count DESC LIMIT 5"
            )
            for appid, name, pc in cur.fetchall():
                print(f"  sample appid={appid} {(name or '')[:30]}: {pc} 人")
    finally:
        conn.close()
    print("保存完了。")


if __name__ == "__main__":
    main()
