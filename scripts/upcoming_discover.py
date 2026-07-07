#!/usr/bin/env python3
"""
「これから来そう」用の発売前ゲーム発見コレクタ ── 2026-07-07 / v1（scaffold）
================================================================
なぜ必要か：既存の収集は「今プレイヤーがいる＝発売済み」から games を発見するため、
プレイヤー0の発売前ゲームが games に一件も入らず、export_upcoming.py が常に 0 件になる。
ここで発売前の appid を games に補充する（＝発見のみ）。

源：store.steampowered.com/api/featuredcategories の "coming_soon"
    （appdetails と同系統の公式ストアJSON。既存パイプラインが使っている系統と同じ）。

書き込み（既存のロスター登録 build_roster.py と同型）：
  - INSERT INTO games (appid, name, status) VALUES %s ON CONFLICT (appid) DO NOTHING
  - 発見分に coming_soon=true を付与（発売日・ジャンルは後続の appdetails_sweep が付与し、
    発売されれば appdetails が coming_soon=false に自動訂正する）。

守る線：スキーマ変更なし・新規は appid と name（公式リンクの素）のみ・著作物は載せない。
実行：.github/workflows/upcoming_discover.yml（当面は手動 workflow_dispatch のみ。動作確認後に schedule 追加）。
env：CC / LANG_STORE / LIMIT。
"""
import json
import os
import sys
import urllib.parse
import urllib.request

import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ["DATABASE_URL"]
CC = os.environ.get("CC", "JP")
LANG_STORE = os.environ.get("LANG_STORE", "japanese")
LIMIT = int(os.environ.get("LIMIT") or "300")
UA = "trend-pulse-upcoming/1.0 (+https://github.com/gakinotukaida-dot/game-site)"


def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def discover():
    """featuredcategories の coming_soon から {appid: name} を得る。"""
    url = "https://store.steampowered.com/api/featuredcategories?" + urllib.parse.urlencode(
        {"cc": CC, "l": LANG_STORE})
    data = fetch_json(url)
    items = ((data or {}).get("coming_soon") or {}).get("items") or []
    out = {}
    for it in items:
        aid = it.get("id")
        name = (it.get("name") or "").strip()
        if isinstance(aid, int) and aid > 0:
            out[aid] = name[:200] or ("appid " + str(aid))
    return out


def main():
    found = discover()
    if not found:
        print("[upcoming] featuredcategories から coming_soon を取得できませんでした（0件・スキップ）。")
        return 0

    pairs = list(found.items())[:LIMIT]
    rows = [(aid, name, "active") for aid, name in pairs]
    appids = [aid for aid, _ in pairs]

    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO games (appid, name, status) VALUES %s ON CONFLICT (appid) DO NOTHING",
                rows,
            )
            ins = cur.rowcount
            cur.execute(
                "UPDATE games SET coming_soon = true "
                "WHERE appid = ANY(%s) AND coming_soon IS DISTINCT FROM true",
                (appids,),
            )
            upd = cur.rowcount
        conn.commit()
    finally:
        conn.close()

    print(f"[upcoming] 発見 {len(found)} 件：新規登録 {ins} 件・coming_soon 付与 {upd} 件。")
    print("           発売日/ジャンルは後続の appdetails_sweep が付与 → export_upcoming が拾えるようになります。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
