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

# 密ティア＝ status='watchlist' ∪「直近観測CCUの上位 DENSE_N 件」∪「直近の自己比急上昇（昇格）」を毎回動的に算出。
# “動き出した新顔”は、絶対では上位N圏外でも、自己比で跳ねていれば昇格条件で密ティアに入る。伸びが収まれば自然に外れる。
DENSE_N = int(os.environ.get("DENSE_N") or "300")            # 絶対CCU上位の件数。暫定。
FLOOR = int(os.environ.get("FLOOR") or "1")                  # この同接以上を保存（daily_sweep と同じ既定=1）
RATE_PER_SEC = int(os.environ.get("RATE_PER_SEC") or "12")   # 毎秒リクエスト上限（429回避の安全側）
WORKERS = int(os.environ.get("WORKERS") or "8")
CCU_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

# --- 昇格（動いた新顔を密へ）のしきい値。すべて暫定・env可変。データが疎なうちは粗い・空振りも正常。---
# 「直近 RISER_RECENT_HOURS の最大CCU」が「それ以前(〜RISER_BASE_DAYS)の平均」に対し ×RISER_MULT 以上、
#  かつ +RISER_ABS_ADD 以上、かつ絶対 RISER_ABS_FLOOR 以上で、基準観測が RISER_MIN_BASE_OBS 点以上 → 昇格。
RISER_RECENT_HOURS = int(os.environ.get("RISER_RECENT_HOURS") or "6")
RISER_BASE_DAYS = int(os.environ.get("RISER_BASE_DAYS") or "14")
RISER_MIN_BASE_OBS = int(os.environ.get("RISER_MIN_BASE_OBS") or "2")
RISER_MULT = float(os.environ.get("RISER_MULT") or "3")
RISER_ABS_ADD = int(os.environ.get("RISER_ABS_ADD") or "200")
RISER_ABS_FLOOR = int(os.environ.get("RISER_ABS_FLOOR") or "200")

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
    """密ティア＝ watchlist ∪ 上位N ∪ 昇格(直近の自己比急上昇)。
    戻り値: (appid のリスト, 昇格条件を満たした件数)。すべて読み取りのみ。"""
    params = {
        "n": DENSE_N,
        "recent_h": RISER_RECENT_HOURS,
        "base_days": RISER_BASE_DAYS,
        "min_base": RISER_MIN_BASE_OBS,
        "mult": RISER_MULT,
        "abs_add": RISER_ABS_ADD,
        "abs_floor": RISER_ABS_FLOOR,
    }
    sql = (
        "WITH latest AS ("
        "  SELECT DISTINCT ON (appid) appid, player_count"
        "  FROM player_counts ORDER BY appid, recorded_at DESC"
        "), top_active AS ("
        "  SELECT l.appid FROM latest l JOIN games g ON g.appid = l.appid"
        "  WHERE g.status <> 'dormant' ORDER BY l.player_count DESC LIMIT %(n)s"
        "), windowed AS ("
        "  SELECT appid,"
        "    max(player_count) FILTER (WHERE recorded_at >= now() - make_interval(hours => %(recent_h)s)) AS recent_max,"
        "    avg(player_count) FILTER (WHERE recorded_at <  now() - make_interval(hours => %(recent_h)s)) AS base_avg,"
        "    count(*)          FILTER (WHERE recorded_at <  now() - make_interval(hours => %(recent_h)s)) AS base_n"
        "  FROM player_counts WHERE recorded_at >= now() - make_interval(days => %(base_days)s)"
        "  GROUP BY appid"
        "), risers AS ("
        "  SELECT w.appid FROM windowed w JOIN games g ON g.appid = w.appid"
        "  WHERE g.status <> 'dormant' AND w.recent_max IS NOT NULL AND w.base_avg IS NOT NULL"
        "    AND w.base_n >= %(min_base)s"
        "    AND w.recent_max >= GREATEST(w.base_avg * %(mult)s, w.base_avg + %(abs_add)s)"
        "    AND w.recent_max >= %(abs_floor)s"
        ") "
        "SELECT appid, bool_or(is_riser) AS is_riser FROM ("
        "  SELECT appid, false AS is_riser FROM games WHERE status = 'watchlist'"
        "  UNION ALL SELECT appid, false FROM top_active"
        "  UNION ALL SELECT appid, true  FROM risers"
        ") u GROUP BY appid"
    )
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            targets = [r[0] for r in rows]
            n_risers = sum(1 for r in rows if r[1])
            return targets, n_risers
    finally:
        conn.close()


def write_results(checked, above):
    """checked=取得できた全 appid（last_checked_at 更新）, above=同接 FLOOR 以上（player_counts 追加）。
    すべて INSERT / 時刻 UPDATE のみ＝非破壊。status は変えない。"""
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
    targets, n_risers = get_targets()
    print(f"密ティア対象: {len(targets)} 件 (内 昇格条件該当 {n_risers} 件) "
          f"[DENSE_N={DENSE_N}, floor={FLOOR}, rate={RATE_PER_SEC}/s, workers={WORKERS}; "
          f"riser x{RISER_MULT}/+{RISER_ABS_ADD}/>={RISER_ABS_FLOOR}, 直近{RISER_RECENT_HOURS}h vs {RISER_BASE_DAYS}d, 基準>={RISER_MIN_BASE_OBS}点]")
    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for appid, pc in ex.map(fetch_ccu, targets):
            results.append((appid, pc))
    checked = [a for (a, pc) in results if pc is not None]
    above = [(a, pc) for (a, pc) in results if pc is not None and pc >= FLOOR]
    fails = len(targets) - len(checked)
    print(f"取得成功: {len(checked)} 件 / 保存(同接{FLOOR}以上): {len(above)} 件 / 取得失敗: {fails} 件")
    write_results(checked, above)
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
