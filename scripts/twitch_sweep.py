"""
twitch_sweep ── Twitch視聴の時系列を保存する収集（書き込み収集）── 2026-06-08
================================================================
役割：dense と同じ焦点集合（watchlist ∪ 上位CCU ∪ 自己比急上昇 ∪ 発売直後14日）のゲームについて、
      Twitch の「現在の視聴者数・配信数」を取得し、隔離テーブル `twitch_snapshots` に**追記保存**する。
      → 「いつもより視聴が多いか」の基準値、および案2の特徴量の土台になる。

【これは“書き込み収集”です（これまでの読み取り専用とは別）】
  - 既存テーブルには一切触れない。**新しい隔離テーブル `twitch_snapshots` のみ**を作成（IF NOT EXISTS）・**追記(INSERT)のみ**。
  - いつでも `DROP TABLE twitch_snapshots;` で元に戻せる（可逆）。
  - 鍵は GitHub Secrets（オーナー発行）。**書き込みの実行はオーナー**（ファイル追加→commit→Actions）。Claudeは実行しない。
  - 法務：依頼資料 v2+v3 を弁護士確認済み（Twitch視聴の保存＝A1基準値）の前提。集計値（ゲーム単位）のみで個人視聴者は扱わない。

保存項目（最小）：appid / viewers（上位100配信の合計・近似） / channels（配信数） / recorded_at。
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

TARGET_N    = int(os.environ.get("TWITCH_TARGET_N") or "500")  # 上位CCUの件数（Twitch率制限を考慮し控えめ）
RISER_RECENT_HOURS = int(os.environ.get("RISER_RECENT_HOURS") or "6")
RISER_BASE_DAYS = int(os.environ.get("RISER_BASE_DAYS") or "14")
RISER_MIN_BASE = int(os.environ.get("RISER_MIN_BASE_OBS") or "2")
RISER_MULT  = float(os.environ.get("RISER_MULT") or "3")
RISER_ABS_ADD = int(os.environ.get("RISER_ABS_ADD") or "200")
RISER_ABS_FLOOR = int(os.environ.get("RISER_ABS_FLOOR") or "200")
LAUNCH_DAYS = int(os.environ.get("LAUNCH_DAYS") or "14")
LAUNCH_MAX  = int(os.environ.get("LAUNCH_MAX") or "200")

DDL = """
CREATE TABLE IF NOT EXISTS twitch_snapshots (
  appid       bigint      NOT NULL,
  viewers     integer     NOT NULL,
  channels    integer     NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS twitch_snapshots_appid_time ON twitch_snapshots (appid, recorded_at DESC);
"""

# dense と同等の焦点集合（appid と name を返す）。store<>'dormant'。
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
    print("twitch_sweep（Twitch視聴の時系列保存・書き込み収集）")
    targets = get_targets()
    print(f"対象（dense同等の焦点集合）: {len(targets)} 件")
    if not targets:
        print("対象0件。終了。")
        return
    token = get_token()

    # name → game_id
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
            rows.append((appid, sum(s.get("viewer_count", 0) for s in streams), len(streams)))
            time.sleep(0.2)
        except Exception as e:
            print(f"  ⚠ streams失敗(appid={appid}): {type(e).__name__}: {e}")

    if not rows:
        print("保存対象0件（マッチなし）。終了。")
        return
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(DDL)  # 隔離テーブル作成（IF NOT EXISTS・既存非干渉）
                execute_values(cur,
                    "INSERT INTO twitch_snapshots (appid, viewers, channels) VALUES %s",
                    rows, page_size=1000)
        print(f"保存: {len(rows)} 件 → twitch_snapshots（追記）。")
    finally:
        conn.close()
    print("=" * 80)
    print("※集計値のみ・個人視聴者は扱わない。元に戻すには DROP TABLE twitch_snapshots;")


if __name__ == "__main__":
    main()
