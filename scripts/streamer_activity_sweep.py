"""
streamer_activity_sweep ── B2: 配信者活動の広域・短期記録（書き込み収集）── 2026-06-08
================================================================
役割：焦点ゲーム（dense同等：watchlist ∪ 上位CCU ∪ 自己比急上昇 ∪ 発売直後14日）を
      「今 配信している配信者」を**広く**記録する。手選びリストは使わない（観測は広く・重みは別レイヤーで抑える/学習）。
      → 「目利きが小型/新作を拾った」を CCU が動く前に捉える先行シグナル＋将来の案2学習の材料。

設計（批判2回を経た確定）：
  - 観測は広い：焦点ゲームを配信中の配信者を記録（リストで絞らない）。
  - 保存は最小・短期：user_id(主)/login/twitch_game_id+name/appid/viewer_count/recorded_at。**14日ローリングで自動削除**。
  - 量の上限：1ゲームあたり上位 TOPK 人（既定25・env可変）。編集判断でなく“量”の上限。小型は配信者が少なく実質全員入る。
  - appid は自前ゲーム→名前→game_id→streams の順で引くので**解決済み**（その焦点ゲームの appid を付与）。

【書き込み収集・PII を扱う】
  - 新しい隔離表 `streamer_activity` のみ作成(IF NOT EXISTS)・追記＋14日より古い行のDELETEのみ。既存は触らない。`DROP TABLE` で可逆。
  - 実在配信者の公開配信活動（公開情報）を最小限・短期で保持。鍵は Secrets（オーナー）。**実行はオーナー**。
  - 法務：依頼資料 v2+v3 を弁護士確認済みの前提。広く観る形は台帳に明記して確認に含める（特定リストでなく焦点ゲームの配信者を広く・14日短期）。
"""

import os
import json
import time
import urllib.parse
import urllib.request

import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ["DATABASE_URL"]
CLIENT_ID = os.environ["TWITCH_CLIENT_ID"]
CLIENT_SECRET = os.environ["TWITCH_CLIENT_SECRET"]

TARGET_N    = int(os.environ.get("TWITCH_TARGET_N") or "500")
TOPK        = int(os.environ.get("STREAMER_TOPK") or "25")   # 1ゲームあたり記録する配信者数（量の上限）
RETAIN_DAYS = int(os.environ.get("RETAIN_DAYS") or "14")     # 短期ローリング保持
RISER_RECENT_HOURS = int(os.environ.get("RISER_RECENT_HOURS") or "6")
RISER_BASE_DAYS = int(os.environ.get("RISER_BASE_DAYS") or "14")
RISER_MIN_BASE = int(os.environ.get("RISER_MIN_BASE_OBS") or "2")
RISER_MULT  = float(os.environ.get("RISER_MULT") or "3")
RISER_ABS_ADD = int(os.environ.get("RISER_ABS_ADD") or "200")
RISER_ABS_FLOOR = int(os.environ.get("RISER_ABS_FLOOR") or "200")
LAUNCH_DAYS = int(os.environ.get("LAUNCH_DAYS") or "14")
LAUNCH_MAX  = int(os.environ.get("LAUNCH_MAX") or "200")

DDL = """
CREATE TABLE IF NOT EXISTS streamer_activity (
  twitch_user_id text        NOT NULL,
  login          text,
  twitch_game_id text,
  game_name      text,
  appid          bigint,
  viewer_count   integer,
  recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS streamer_activity_user_time ON streamer_activity (twitch_user_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS streamer_activity_appid_time ON streamer_activity (appid, recorded_at DESC);
"""

Q_TARGETS = """
WITH latest AS (
  SELECT DISTINCT ON (appid) appid, player_count FROM player_counts ORDER BY appid, recorded_at DESC
),
top_active AS (
  SELECT l.appid FROM latest l JOIN games g ON g.appid=l.appid
  WHERE g.status <> 'dormant' ORDER BY l.player_count DESC LIMIT %(n)s
),
windowed AS (
  SELECT appid,
    max(player_count) FILTER (WHERE recorded_at >= now() - make_interval(hours => %(recent_h)s)) AS recent_max,
    avg(player_count) FILTER (WHERE recorded_at <  now() - make_interval(hours => %(recent_h)s)) AS base_avg,
    count(*)          FILTER (WHERE recorded_at <  now() - make_interval(hours => %(recent_h)s)) AS base_n
  FROM player_counts WHERE recorded_at >= now() - make_interval(days => %(base_days)s) GROUP BY appid
),
risers AS (
  SELECT w.appid FROM windowed w JOIN games g ON g.appid=w.appid
  WHERE g.status <> 'dormant' AND w.recent_max IS NOT NULL AND w.base_avg IS NOT NULL
    AND w.base_n >= %(min_base)s
    AND w.recent_max >= GREATEST(w.base_avg * %(mult)s, w.base_avg + %(abs_add)s)
    AND w.recent_max >= %(abs_floor)s
),
launched AS (
  SELECT g.appid FROM games g
  WHERE g.status <> 'dormant' AND g.coming_soon IS FALSE AND g.release_date IS NOT NULL
    AND g.release_date >= (now()::date - %(launch_days)s)
  ORDER BY g.release_date DESC LIMIT %(launch_max)s
)
SELECT DISTINCT u.appid, g.name FROM (
  SELECT appid FROM games WHERE status='watchlist'
  UNION SELECT appid FROM top_active
  UNION SELECT appid FROM risers
  UNION SELECT appid FROM launched
) u JOIN games g ON g.appid=u.appid;
"""


def get_targets():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute(Q_TARGETS, {"n": TARGET_N, "recent_h": RISER_RECENT_HOURS,
                                    "base_days": RISER_BASE_DAYS, "min_base": RISER_MIN_BASE,
                                    "mult": RISER_MULT, "abs_add": RISER_ABS_ADD,
                                    "abs_floor": RISER_ABS_FLOOR, "launch_days": LAUNCH_DAYS,
                                    "launch_max": LAUNCH_MAX})
            return [(a, n) for a, n in cur.fetchall()]
    finally:
        conn.close()


def norm_name(name):
    import re
    s = re.sub(r"[®™©]", "", (name or "").strip())
    s = re.sub(r"\s*:\s*.*\bEdition\b.*$", "", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip()


def _get(url, token):
    req = urllib.request.Request(url, headers={"Client-Id": CLIENT_ID, "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def get_token():
    data = urllib.parse.urlencode({"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
                                   "grant_type": "client_credentials"}).encode()
    with urllib.request.urlopen("https://id.twitch.tv/oauth2/token", data=data, timeout=30) as r:
        return json.load(r)["access_token"]


def main():
    print("=" * 80)
    print("streamer_activity_sweep（B2: 配信者活動の広域・短期記録・書き込み）")
    targets = get_targets()
    print(f"対象（焦点集合）: {len(targets)} 件 / 各ゲーム上位{TOPK}人 / 保持{RETAIN_DAYS}日")
    if not targets:
        print("対象0件。終了。")
        return
    token = get_token()

    name_by_norm = {}
    for _a, n in targets:
        if n:
            name_by_norm.setdefault(norm_name(n).lower(), n)
    norms = list(name_by_norm.keys())
    name_to_id = {}
    for i in range(0, len(norms), 100):
        qs = "&".join("name=" + urllib.parse.quote(b) for b in norms[i:i + 100])
        try:
            for g in _get("https://api.twitch.tv/helix/games?" + qs, token).get("data", []):
                name_to_id[g["name"].lower()] = g["id"]
        except Exception as e:
            print(f"  ⚠ games lookup失敗: {type(e).__name__}: {e}")
        time.sleep(0.2)

    rows = []
    for appid, name in targets:
        gid = name_to_id.get(norm_name(name).lower()) if name else None
        if not gid:
            continue
        try:
            streams = _get(f"https://api.twitch.tv/helix/streams?game_id={gid}&first=100", token).get("data", [])
        except Exception as e:
            print(f"  ⚠ streams失敗(appid={appid}): {type(e).__name__}: {e}")
            continue
        for s in streams[:TOPK]:  # 既に viewer_count 降順。上位K人（量の上限）。
            rows.append((s.get("user_id"), s.get("user_login"), gid,
                         s.get("game_name") or name, appid, s.get("viewer_count", 0)))
        time.sleep(0.2)

    if not rows:
        print("記録対象0件（マッチなし）。終了。")
        return
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL)  # 隔離表（IF NOT EXISTS）
                execute_values(cur,
                    "INSERT INTO streamer_activity "
                    "(twitch_user_id, login, twitch_game_id, game_name, appid, viewer_count) VALUES %s",
                    rows, page_size=2000)
                cur.execute("DELETE FROM streamer_activity WHERE recorded_at < now() - make_interval(days => %s)",
                            (RETAIN_DAYS,))
                pruned = cur.rowcount
        print(f"記録: {len(rows)} 行 → streamer_activity（追記）。{RETAIN_DAYS}日より古い {pruned} 行を削除。")
    finally:
        conn.close()
    print("=" * 80)
    print("※公開配信の最小限・短期保持。目利きスコアはリストでなくデータから後学習。元に戻すには DROP TABLE streamer_activity;")


if __name__ == "__main__":
    main()
