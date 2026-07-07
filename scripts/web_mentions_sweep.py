"""
Web上の言及数カウント収集（分離テーブル・追記のみ）── 2026-07-07 / v1
================================================================
役割：そのゲームについて Web 上で「数えられるもの（記事数・動画数など）」を数えて web_mentions に貯める。
      将来 羽根予想の特徴量に加える（有意かどうかはモデルの較正が判断＝集めるだけでよい）。

線（既存に無影響・可逆）：
- **分離テーブル web_mentions のみ**を CREATE IF NOT EXISTS し、**INSERT のみ**。既存テーブルは一切触らない。
- 戻すのはこのファイル/ワークフローを消すだけ（web_mentions が古くなるだけ・他に影響なし）。
- 成人向けは数えない（_filters.not_adult）。

ソース（プラガブル・失敗は各ソースで握りつぶしてスキップ＝1つ壊れても他は動く）：
- youtube : YOUTUBE_API_KEY があれば search の推定総数（pageInfo.totalResults）を記録。公式API・無料枠。
- note    : 認証不要（非公式の検索JSON）。記事数を best-effort で記録。※非公式のため壊れたら0でスキップ。
  → 追加は SOURCES に関数を足すだけ（「ネット上で数えられるものなら何でも」拡張できる設計）。

対象：発売前(coming_soon)を優先して件数を絞る（YouTube 無料枠＝検索1回100units・1日1万→約100件/日）。
env：MENTIONS_CAP（1回の対象上限・既定50）／YOUTUBE_API_KEY／MENTIONS_SOURCES（既定 "youtube,note"）／HTTP_TIMEOUT。
"""

import os
import json
import time
import urllib.parse
import urllib.request
import urllib.error

import psycopg2
from psycopg2.extras import execute_batch

from _filters import not_adult

DATABASE_URL = os.environ["DATABASE_URL"]
MENTIONS_CAP = int(os.environ.get("MENTIONS_CAP") or "50")
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT") or "12")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY") or ""
SOURCES = [s.strip() for s in (os.environ.get("MENTIONS_SOURCES") or "youtube,note").split(",") if s.strip()]
UA = "trepa-web-mentions/1.0 (+https://github.com/gakinotukaida-dot/game-site)"

DDL = """
CREATE TABLE IF NOT EXISTS web_mentions (
  appid       bigint      NOT NULL,
  source      text        NOT NULL,
  mentions    bigint,
  recorded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS web_mentions_appid_time ON web_mentions (appid, recorded_at DESC);
"""

TARGET_QUERY = """
SELECT g.appid, g.name
FROM games g
WHERE g.coming_soon IS TRUE
  AND g.name IS NOT NULL
  AND {na}
ORDER BY g.release_date ASC NULLS LAST, g.appid
LIMIT %(cap)s
""".format(na=not_adult("g"))


def _get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def src_youtube(name):
    """YouTube Data API search の推定総ヒット数（動画）。要 YOUTUBE_API_KEY。"""
    if not YOUTUBE_API_KEY:
        return None
    q = urllib.parse.urlencode({"part": "snippet", "type": "video", "maxResults": "1",
                                "q": name, "key": YOUTUBE_API_KEY})
    data = _get_json("https://www.googleapis.com/youtube/v3/search?" + q)
    pi = (data or {}).get("pageInfo") or {}
    return int(pi.get("totalResults") or 0)


def src_note(name):
    """note の検索ヒット記事数（非公式JSON・best-effort）。壊れたら例外→スキップ。"""
    q = urllib.parse.urlencode({"context": "note", "q": name, "size": "1", "start": "0"})
    data = _get_json("https://note.com/api/v3/searches?" + q)
    d = (data or {}).get("data") or {}
    # 既知の形に順に当てる（版によって differ するため寛容に）
    for path in (("notes", "total_count"), ("total_count",), ("notes", "totalCount")):
        cur = d
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and isinstance(cur, int):
            return int(cur)
    return 0


SOURCE_FUNCS = {"youtube": src_youtube, "note": src_note}


def _write(sql_fn):
    """書き込みを実行。Neon サーバーレスの復帰時に一時的な read-only 窓（SQLSTATE 25006）や
    接続断が起きうるので、指数バックオフで数回だけ再接続・再試行する（appdetails と同じ方針）。"""
    attempts = int(os.environ.get("WRITE_RETRIES") or "4")
    for i in range(attempts):
        conn = None
        try:
            conn = psycopg2.connect(DATABASE_URL)
            with conn, conn.cursor() as cur:
                sql_fn(cur)
            return
        except psycopg2.Error as e:
            transient = (getattr(e, "pgcode", None) == "25006") or isinstance(e, psycopg2.OperationalError)
            if not transient or i == attempts - 1:
                raise
            print(f"[retry] 一時的な read-only/接続断（{getattr(e, 'pgcode', '')}）→ {3*(2**i)}s 後に再試行 ({i+1}/{attempts})")
            time.sleep(3 * (2 ** i))
        finally:
            if conn is not None:
                conn.close()


def ensure_table():
    _write(lambda cur: cur.execute(DDL))


def get_targets():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(TARGET_QUERY, {"cap": MENTIONS_CAP})
            return cur.fetchall()
    finally:
        conn.close()


def main():
    ensure_table()
    targets = get_targets()
    active = [s for s in SOURCES if s in SOURCE_FUNCS and (s != "youtube" or YOUTUBE_API_KEY)]
    skipped = [s for s in SOURCES if s not in active]
    print(f"対象 {len(targets)} 件・ソース {active}"
          + (f"（未設定でスキップ: {skipped}）" if skipped else ""))
    if not targets or not active:
        if not active:
            print("有効なソースがありません（YOUTUBE_API_KEY 未設定＋note無効など）。何もしません。")
        return

    rows = []
    shown = 0
    for appid, name in targets:
        for s in active:
            try:
                n = SOURCE_FUNCS[s](name)
            except (urllib.error.URLError, urllib.error.HTTPError, ValueError, TimeoutError, OSError) as e:
                if shown < 6:
                    print(f"  [skip] {s} appid={appid} '{(name or '')[:30]}': {type(e).__name__}")
                n = None
            if n is not None:
                rows.append((appid, s, int(n)))
                if shown < 12:
                    print(f"  {s:8} {appid:>9} {int(n):>8}  {(name or '')[:40]}")
                    shown += 1
            time.sleep(0.2)  # 軽い間隔（各APIに優しく）

    if not rows:
        print("記録行 0（全ソースが0/失敗）。")
        return

    _write(lambda cur: execute_batch(
        cur, "INSERT INTO web_mentions (appid, source, mentions) VALUES (%s, %s, %s)",
        rows, page_size=500))

    per = {}
    for _, s, n in rows:
        per[s] = per.get(s, 0) + 1
    print(f"記録: {len(rows)} 行（ソース別 {per}）")


if __name__ == "__main__":
    main()
