import os
import json
import urllib.parse
import urllib.request

import psycopg2
from psycopg2.extras import execute_values

STEAM_API_KEY = os.environ["STEAM_API_KEY"]
DATABASE_URL = os.environ["DATABASE_URL"]
API = "https://api.steampowered.com/IStoreService/GetAppList/v1/"


def fetch_all_games():
    games = {}
    last_appid = 0
    while True:
        params = {
            "key": STEAM_API_KEY,
            "include_games": "true",
            "include_dlc": "false",
            "include_software": "false",
            "include_videos": "false",
            "include_hardware": "false",
            "max_results": "50000",
        }
        if last_appid:
            params["last_appid"] = str(last_appid)
        with urllib.request.urlopen(API + "?" + urllib.parse.urlencode(params), timeout=60) as r:
            data = json.load(r)
        resp = data.get("response", {})
        apps = resp.get("apps", [])
        for a in apps:
            appid = a.get("appid")
            name = (a.get("name") or "").strip()
            if appid and name:
                games[appid] = name
        if resp.get("have_more_results") and resp.get("last_appid"):
            last_appid = resp["last_appid"]
        else:
            break
    return games


def main():
    games = fetch_all_games()
    print(f"Steamから取得したゲーム数: {len(games)}")
    rows = [(appid, name, "active") for appid, name in games.items()]
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO games (appid, name, status) VALUES %s ON CONFLICT (appid) DO NOTHING",
                    rows,
                    page_size=1000,
                )
                cur.execute("SELECT count(*) FROM games;")
                total = cur.fetchone()[0]
                cur.execute("SELECT status, count(*) FROM games GROUP BY status ORDER BY count(*) DESC;")
                by_status = cur.fetchall()
        print(f"名簿(games)の総数: {total}")
        for status, cnt in by_status:
            print(f"  {status}: {cnt}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
