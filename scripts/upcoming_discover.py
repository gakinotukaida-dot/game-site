#!/usr/bin/env python3
"""
「これから来そう」用の発売前ゲーム発見コレクタ ── 2026-07-08 / v2（大量発見）
================================================================
なぜ必要か：既存の収集は「今プレイヤーがいる＝発売済み」から games を発見するため、
プレイヤー0の発売前ゲームが games に入らず、羽根予想の対象が数件しか無かった。
ここで発売前の appid を大量に補充する（＝発見のみ・予測対象と学習ペアを増やす）。

源（どちらも公式ストアの公開JSON。appid＝公式リンクの素のみ取得。著作物は載せない）：
  1) featuredcategories の "coming_soon"（厳選ハイライト・十数件）。
  2) ストア検索 "近日登場"（filter=comingsoon）を **ページング**して大量取得（数百件）。
     ※ ストアサイト自身が使う AJAX エンドポイント。results_html から data-ds-appid と題名を取り出す。

書き込み（build_roster.py と同型・冪等）：
  - INSERT INTO games (appid, name, status) ... ON CONFLICT DO NOTHING
  - 発見分に coming_soon=true を付与（発売日/ジャンル/開発元/体験版は後続の appdetails_sweep が付与、
    発売されれば appdetails が coming_soon=false に自動訂正）。
  - Neon の scale-to-zero 復帰時の一時 read-only(25006) に指数バックオフで再試行。

守る線：スキーマ変更なし・新規は appid と name のみ・可逆（消せば発見が止まるだけ）。
env：CC / LANG_STORE / LIMIT（登録上限・既定500）/ SEARCH_PAGES / SEARCH_COUNT / SEARCH_DELAY。
"""
import os
import re
import sys
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from html import unescape

import psycopg2
from psycopg2.extras import execute_values

DATABASE_URL = os.environ["DATABASE_URL"]
CC = os.environ.get("CC", "JP")
LANG_STORE = os.environ.get("LANG_STORE", "japanese")
LIMIT = int(os.environ.get("LIMIT") or "500")
SEARCH_PAGES = int(os.environ.get("SEARCH_PAGES") or "12")   # 最大ページ数（×COUNT が上限件数）
SEARCH_COUNT = int(os.environ.get("SEARCH_COUNT") or "100")  # 1ページ件数
SEARCH_DELAY = float(os.environ.get("SEARCH_DELAY") or "1.5")  # ページ間の間隔（ストアに優しく）
UA = "trend-pulse-upcoming/1.0 (+https://github.com/gakinotukaida-dot/game-site)"

_APPID_CHUNK = re.compile(r'^(\d+)')
_TITLE_RE = re.compile(r'<span class="title">(.*?)</span>', re.S)


def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def discover_featured():
    """featuredcategories の coming_soon（厳選ハイライト）から {appid: name}。"""
    url = "https://store.steampowered.com/api/featuredcategories?" + urllib.parse.urlencode(
        {"cc": CC, "l": LANG_STORE})
    try:
        data = fetch_json(url)
    except (urllib.error.URLError, ValueError, OSError) as e:
        print(f"[upcoming][featured] 取得失敗: {type(e).__name__}")
        return {}
    items = ((data or {}).get("coming_soon") or {}).get("items") or []
    out = {}
    for it in items:
        aid = it.get("id")
        name = (it.get("name") or "").strip()
        if isinstance(aid, int) and aid > 0:
            out[aid] = name[:200]
    return out


def discover_search():
    """ストア検索 "近日登場" をページングして {appid: name} を大量に集める。"""
    out = {}
    for page in range(SEARCH_PAGES):
        start = page * SEARCH_COUNT
        url = "https://store.steampowered.com/search/results/?" + urllib.parse.urlencode({
            "filter": "comingsoon", "start": start, "count": SEARCH_COUNT,
            "cc": CC, "l": LANG_STORE, "infinite": "1",
        })
        try:
            data = fetch_json(url)
        except (urllib.error.URLError, ValueError, OSError) as e:
            print(f"[upcoming][search] page {page} 取得失敗: {type(e).__name__} → 打ち切り")
            break
        html = (data or {}).get("results_html") or ""
        total = int((data or {}).get("total_count") or 0)
        n_before = len(out)
        # results_html を data-ds-appid で分割し、各行の appid と <span class="title"> を対応付け
        for chunk in html.split('data-ds-appid="')[1:]:
            m = _APPID_CHUNK.match(chunk)
            if not m:
                continue
            aid = int(m.group(1))
            if aid <= 0:
                continue
            tm = _TITLE_RE.search(chunk)
            name = unescape(tm.group(1)).strip() if tm else ""
            out.setdefault(aid, name[:200])
        got = len(out) - n_before
        print(f"[upcoming][search] page {page} start={start} total={total} 追加 {got} 件（累計 {len(out)}）")
        if got == 0 or (total and start + SEARCH_COUNT >= total):
            break
        time.sleep(SEARCH_DELAY)
    return out


def _write_games(rows, appids):
    """games へ登録＋coming_soon 付与。Neon の一時 read-only(25006)/接続断に指数バックオフ再試行。"""
    attempts = int(os.environ.get("WRITE_RETRIES") or "6")
    for i in range(attempts):
        conn = None
        try:
            conn = psycopg2.connect(DATABASE_URL)
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
            return ins, upd
        except psycopg2.Error as e:
            if conn is not None:
                conn.rollback()
            transient = (getattr(e, "pgcode", None) == "25006") or isinstance(e, psycopg2.OperationalError)
            if not transient or i == attempts - 1:
                raise
            wait = min(30, 3 * (2 ** i))
            print(f"[retry] 一時 read-only/接続断（{getattr(e, 'pgcode', '')}）→ {wait}s 後に再試行 ({i+1}/{attempts})")
            time.sleep(wait)
        finally:
            if conn is not None:
                conn.close()


def main():
    found = discover_search()
    for aid, name in discover_featured().items():  # 厳選ハイライトを上書き（名前が良い）
        found[aid] = name or found.get(aid, "")
    if not found:
        print("[upcoming] 発見0件（ストア検索/featuredcategories とも取得できず・スキップ）。")
        return 0

    pairs = list(found.items())[:LIMIT]
    rows = [(aid, (name or ("appid " + str(aid)))[:200], "active") for aid, name in pairs]
    appids = [aid for aid, _ in pairs]

    ins, upd = _write_games(rows, appids)
    print(f"[upcoming] 発見 {len(found)} 件（登録上限 {LIMIT}）：新規登録 {ins} 件・coming_soon 付与 {upd} 件。")
    print("           発売日/ジャンル/開発元/体験版は後続の appdetails_sweep が付与 → 羽根予想の対象になります。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
