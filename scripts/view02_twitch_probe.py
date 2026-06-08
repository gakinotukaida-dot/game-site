"""
view02 Twitch lite プローブ ── 読み取り専用・保存なし ── 2026-06-08
================================================================
役割：view02 上位ゲームについて、Twitch の「今の配信注目（視聴者数・配信数）」を取得し、
      ランキングに併記する（＝推定きっかけ「配信」の材料 / B1 lite）。
      これは最小リスク版：**Twitchデータを保存せず、計算時に取得して使い捨てる**
      （長期保存・DB化は ToU の弁護士確認後＝別段階）。

安全性：
  - 自前DBは read-only 固定・SELECTのみ（書込なし）。
  - Twitchデータは**保存しない**（印字のみ）。表示・保存・再配布は別途・弁護士。
  - 認証は client_credentials（アプリトークン）。**鍵は GitHub Secrets（オーナー発行）**＝コードに書かない。

必要な環境変数（Secrets）：
  DATABASE_URL（既存）／TWITCH_CLIENT_ID／TWITCH_CLIENT_SECRET（オーナーが Twitch でアプリ登録して発行）

確認済みの取り方（dev.twitch.tv）：
  token  : POST https://id.twitch.tv/oauth2/token  (client_id, client_secret, grant_type=client_credentials)
  games  : GET  https://api.twitch.tv/helix/games?name=...   （最大100名・name→id）
  streams: GET  https://api.twitch.tv/helix/streams?game_id=...&first=100  （viewer_count降順）
"""

import os
import re
import json
import time
import urllib.parse
import urllib.request

import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

BASE_DAYS   = int(os.environ.get("BASE_DAYS")   or "14")
GAP_DAYS    = int(os.environ.get("GAP_DAYS")    or "1")
N0          = float(os.environ.get("N0")        or "10")
MIN_CURRENT = int(os.environ.get("MIN_CURRENT") or "100")
MIN_POINTS  = int(os.environ.get("MIN_POINTS")  or "5")
TOP_N       = int(os.environ.get("TOP_N")       or "30")

Q_TOP = """
WITH latest AS (
  SELECT DISTINCT ON (appid) appid, player_count AS current_ccu
  FROM player_counts ORDER BY appid, recorded_at DESC
),
base AS (
  SELECT appid, percentile_cont(0.5) WITHIN GROUP (ORDER BY player_count) AS baseline, count(*) AS n_points
  FROM player_counts
  WHERE recorded_at <  now() - make_interval(days => %(gap_days)s)
    AND recorded_at >= now() - make_interval(days => %(base_days)s)
  GROUP BY appid
)
SELECT l.appid, g.name, l.current_ccu,
       1 + (b.n_points::float/(b.n_points+%(n0)s))*(l.current_ccu::float/NULLIF(b.baseline,0)-1) AS shrunk_ratio
FROM latest l JOIN games g ON g.appid=l.appid LEFT JOIN base b ON b.appid=l.appid
WHERE l.current_ccu >= %(min_current)s AND b.n_points >= %(min_points)s
ORDER BY shrunk_ratio DESC NULLS LAST
LIMIT %(top_n)s;
"""


def get_top_games():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(Q_TOP, {"gap_days": GAP_DAYS, "base_days": BASE_DAYS, "n0": N0,
                                "min_current": MIN_CURRENT, "min_points": MIN_POINTS, "top_n": TOP_N})
            return [{"appid": a, "name": n, "current": c, "ratio": r} for a, n, c, r in cur.fetchall()]
    finally:
        conn.close()


def norm_name(name):
    s = (name or "").strip()
    s = re.sub(r"[®™©]", "", s)
    s = re.sub(r"\s*:\s*.*\bEdition\b.*$", "", s, flags=re.I)  # 「: 2026 Edition」等を落とす
    return re.sub(r"\s+", " ", s).strip()


def _get(url, token):
    req = urllib.request.Request(url, headers={
        "Client-Id": CLIENT_ID, "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def get_token():
    data = urllib.parse.urlencode({
        "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"}).encode()
    with urllib.request.urlopen("https://id.twitch.tv/oauth2/token", data=data, timeout=30) as r:
        return json.load(r)["access_token"]


def map_names_to_ids(names, token):
    """helix/games?name=... を最大100件ずつ。Twitchのカテゴリ名（正規化）→ game_id。"""
    out = {}
    uniq = list({norm_name(n) for n in names if n})
    for i in range(0, len(uniq), 100):
        batch = uniq[i:i + 100]
        qs = "&".join("name=" + urllib.parse.quote(b) for b in batch)
        try:
            data = _get("https://api.twitch.tv/helix/games?" + qs, token)
            for g in data.get("data", []):
                out[g["name"].lower()] = g["id"]
        except Exception as e:
            print(f"  ⚠ games lookup 失敗: {type(e).__name__}: {e}")
        time.sleep(0.2)
    return out


def viewers_for_game(game_id, token):
    """streams?game_id=...&first=100 の1ページ合計（上位100配信＝視聴の大半・近似）。"""
    try:
        data = _get(f"https://api.twitch.tv/helix/streams?game_id={game_id}&first=100", token)
        streams = data.get("data", [])
        return sum(s.get("viewer_count", 0) for s in streams), len(streams)
    except Exception as e:
        print(f"  ⚠ streams 失敗(game_id={game_id}): {type(e).__name__}: {e}")
        return None, None


def main():
    print("=" * 80)
    print("view02 Twitch lite プローブ（読み取り専用・Twitchデータ保存なし）")
    if not CLIENT_ID or not CLIENT_SECRET:
        print("✗ TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET が未設定です。")
        print("  → Twitch で開発者アプリを登録し、Client ID と Secret を GitHub Secrets に追加してください。")
        print("    （鍵の入力はオーナーが行う。Claudeは入力しない）")
        return
    print("=" * 80)

    games = get_top_games()
    if not games:
        print("上位ゲームが取得できません（MIN_*を緩める/窓を短く）。")
        return
    token = get_token()
    name_to_id = map_names_to_ids([g["name"] for g in games], token)

    print(f"\nview02 上位 {len(games)} 件 × Twitch 現在視聴：")
    print("  name                         現在CCU 倍率   Twitch視聴  配信数  視聴/CCU")
    matched = 0
    for g in games:
        gid = name_to_id.get(norm_name(g["name"]).lower())
        if gid:
            matched += 1
            v, ch = viewers_for_game(gid, token)
            time.sleep(0.2)
        else:
            v, ch = None, None
        nm = (g["name"] or str(g["appid"]))[:27].ljust(27)
        ratio = "—" if g["ratio"] is None else f"{g['ratio']:.2f}"
        vs = "—(未マッチ)" if v is None else str(v)
        chs = "—" if ch is None else str(ch)
        vpc = "" if (v is None or not g["current"]) else f"{v / g['current']:.2f}"
        print(f"  {nm} {str(g['current']).rjust(7)} {ratio.rjust(5)} {vs.rjust(11)} {chs.rjust(6)}   {vpc}")
    print(f"\nマッチ率: {matched}/{len(games)}（Twitchカテゴリ名と一致した数）。")
    print("※視聴は上位100配信の合計＝近似。Twitchデータは保存していません（印字のみ）。")
    print("※公開表示・長期保存・再配布は別段階＝ToU/弁護士の確認後（方針A・内部専用論点）。")
    print("=" * 80)
    print("この出力を共有 → マッチ率と『視聴/CCU』で配信きっかけの効き方を確認 → 合成方法(乗算ブースト＋ラベル)へ。")


if __name__ == "__main__":
    main()
