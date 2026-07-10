"""
Web上の言及数カウント収集（分離テーブル・追記のみ）── 2026-07-07 / v1
================================================================
役割：そのゲームについて Web 上で「数えられるもの（記事数・動画数など）」を数えて web_mentions に貯める。
      将来 羽根予想の特徴量に加える（有意かどうかはモデルの較正が判断＝集めるだけでよい）。

線（既存に無影響・可逆）：
- **分離テーブル web_mentions のみ**を CREATE IF NOT EXISTS し、**INSERT のみ**。既存テーブルは一切触らない。
- 戻すのはこのファイル/ワークフローを消すだけ（web_mentions が古くなるだけ・他に影響なし）。
- 成人向けは数えない（_filters.not_adult）。

ソース（プラガブル・失敗は各ソースで握りつぶしてスキップ＝1つ壊れても他は動く。追加は SOURCE_FUNCS に関数を足すだけ）：
理想＝「世界中の記事をまんべんなく偏りなく」。地域に偏らない“世界横断”系を主軸に、地域/コミュニティ寄りは補助。
- gdelt             : 認証不要。**世界の多言語ニュース**（65言語超）直近1週間の記事数＝最も地域中立。公式API。
- wikipedia_pageviews: 認証不要。**全言語版Wikipediaの直近ページビュー合計**＝地域に偏らない“実際の閲覧＝関心”の総量
                       （件数ではなく人が読んだ回数＝最も強い世界横断シグナル。1作品で言語数ぶんAPIを呼ぶため重い）。公式 Pageviews API。
- wikidata_sitelinks: 認証不要。項目が持つ**言語版Wikipediaの数**＝言語横断の世界的知名度（特定言語に依存しない）。公式API。
- wikipedia_en      : 認証不要。Wikipedia(英)検索の総ヒット数＝英語圏の知名度。公式API。
- wikipedia_ja      : 認証不要。Wikipedia(日)検索の総ヒット数＝国内の知名度。公式API。
- hackernews        : 認証不要。Hacker News(Algolia)の nbHits＝英語圏・技術者寄り（偏りあり・補助）。公式API。
- youtube           : 要 YOUTUBE_API_KEY。search の推定総ヒット数（動画・世界）。公式API・無料枠。
- note              : 認証不要だが 403 で自動アクセス不可＝既定外。

対象：発売前(coming_soon)を優先して件数を絞る（YouTube 無料枠＝検索1回100units・1日1万→約100件/日）。
env：MENTIONS_CAP（既定50）／YOUTUBE_API_KEY／MENTIONS_SOURCES（既定 全6ソース）／HTTP_TIMEOUT／DRY_RUN（trueで収集のみ・DB書き込みなし）。
"""

import os
import re
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_batch

from _filters import not_adult

DATABASE_URL = os.environ["DATABASE_URL"]
MENTIONS_CAP = int(os.environ.get("MENTIONS_CAP") or "50")
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT") or "12")
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY") or ""
PAGEVIEW_DAYS = int(os.environ.get("PAGEVIEW_DAYS") or "14")   # 全言語ページビューを合計する直近日数
MAX_PV_LANGS = int(os.environ.get("MAX_PV_LANGS") or "40")     # 1作品でページビューを引く言語版の上限（暴走防止）
# 「世界中の記事をまんべんなく」の理想に近い順（GDELT=世界の多言語ニュース、Wikidata=言語横断の知名度）を先頭に。
# note は自動アクセスを 403 で弾くため既定から外す（明示指定で試すことは可能）。
_DEFAULT_SOURCES = "gdelt,wikidata_sitelinks,youtube,wikipedia_en,wikipedia_ja,hackernews"
SOURCES = [s.strip() for s in (os.environ.get("MENTIONS_SOURCES") or _DEFAULT_SOURCES).split(",") if s.strip()]
DRY_RUN = (os.environ.get("DRY_RUN") or "").strip().lower() in ("1", "true", "yes", "on")
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


# Wikipedia は「bot を名乗る説明的UA」を推奨、note 等は逆に bot UA を 403 で弾くことがあるためブラウザ風UAで取る。
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/122.0 Safari/537.36")


def _clean_name(name):
    """検索の一致率を上げる軽い正規化。™®© を除去し、"| …"（版/サブラベル）以降を落とす。
    ※ 短くなりすぎない範囲だけ（3文字以上残るときのみ）。コロンのサブタイトルは残す（作品の特定性を保つ）。"""
    s = re.sub(r"[™®©]", "", str(name or ""))
    if "|" in s:
        head = s.split("|")[0].strip()
        if len(head) >= 3:
            s = head
    s = re.sub(r"\s+", " ", s).strip()
    return s or str(name or "")


def _get_json(url, ua=None, timeout=None):
    req = urllib.request.Request(url, headers={"User-Agent": ua or UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=(timeout or HTTP_TIMEOUT)) as r:
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
    """note の検索ヒット記事数（非公式JSON・best-effort）。bot UA を弾くことがあるためブラウザ風UAで取る。"""
    q = urllib.parse.urlencode({"context": "note", "q": name, "size": "1", "start": "0"})
    data = _get_json("https://note.com/api/v3/searches?" + q, ua=BROWSER_UA)
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


def _src_wikipedia(name, lang):
    """Wikipedia 検索の総ヒット数（searchinfo.totalhits）＝知名度/注目の代理。公式API・キー不要。"""
    q = urllib.parse.urlencode({"action": "query", "list": "search", "srsearch": name,
                                "srlimit": "1", "srinfo": "totalhits", "srprop": "", "format": "json"})
    data = _get_json(f"https://{lang}.wikipedia.org/w/api.php?" + q)
    si = ((data or {}).get("query") or {}).get("searchinfo") or {}
    return int(si.get("totalhits") or 0)


def src_wikipedia_en(name):
    return _src_wikipedia(name, "en")


def src_wikipedia_ja(name):
    return _src_wikipedia(name, "ja")


def src_hackernews(name):
    """Hacker News（Algolia API）の総ヒット数（nbHits）＝掲示板/ディスカッションの話題数。公式API・キー不要。"""
    q = urllib.parse.urlencode({"query": name, "tags": "story"})
    data = _get_json("https://hn.algolia.com/api/v1/search?" + q)
    return int((data or {}).get("nbHits") or 0)


_WD_NONLANG = {"commonswiki", "specieswiki", "metawiki", "wikidatawiki", "sourceswiki",
               "mediawikiwiki", "wikimaniawiki", "outreachwiki", "incubatorwiki", "foundationwiki"}


_WD_CACHE = {}  # name -> {langwiki: 記事タイトル}（1回のrun内で Wikidata 呼び出しを共有＝重複APIを避ける）


def _wd_sitelinks_map(name):
    """Wikidata で name に一致する項目の sitelinks を {langwiki: 記事タイトル} で返す（言語版Wikipediaのみ）。
    wikidata_sitelinks（件数）と wikipedia_pageviews（各言語の実閲覧）が共有＝Wikidata呼び出しを1作品1回に。
    見つからなければ {}。ネットワーク例外は呼び出し側（main の per-source try）へ伝播（＝そのソースだけスキップ）。"""
    if name in _WD_CACHE:
        return _WD_CACHE[name]
    out = {}
    q1 = urllib.parse.urlencode({"action": "wbsearchentities", "search": name, "language": "en",
                                 "type": "item", "limit": "1", "format": "json"})
    d1 = _get_json("https://www.wikidata.org/w/api.php?" + q1)
    hits = (d1 or {}).get("search") or []
    qid = hits[0].get("id") if hits else None
    if qid:
        q2 = urllib.parse.urlencode({"action": "wbgetentities", "ids": qid, "props": "sitelinks", "format": "json"})
        d2 = _get_json("https://www.wikidata.org/w/api.php?" + q2)
        links = (((d2 or {}).get("entities") or {}).get(qid) or {}).get("sitelinks") or {}
        # 言語版Wikipedia＝キーが "wiki" で終わる（commons/species/meta等の非言語wikiは除外）
        for k, v in links.items():
            if k.endswith("wiki") and k not in _WD_NONLANG:
                title = (v or {}).get("title")
                if title:
                    out[k] = title
    _WD_CACHE[name] = out
    return out


def src_wikidata_sitelinks(name):
    """Wikidata に一致する項目が“いくつの言語版 Wikipedia に記事を持つか”＝地域に偏らない世界的な知名度。
    特定言語に依存しないので「世界中でまんべんなく」の理想に最も近い1指標。公式API・キー不要。"""
    return len(_wd_sitelinks_map(name))


def _pageviews_range(project, title, start_date, end_date):
    """指定 project（例 en.wikipedia）の記事 title の [start_date, end_date] 合計ページビュー（人間アクセスのみ）。
    その言語版に閲覧データが無い(404)は0。公式 Wikimedia Pageviews API・キー不要（過去の任意期間も引ける）。"""
    art = urllib.parse.quote(title.replace(" ", "_"), safe="")
    url = (f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
           f"{project}/all-access/user/{art}/daily/{start_date:%Y%m%d}/{end_date:%Y%m%d}")
    try:
        data = _get_json(url)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return 0
        raise
    items = (data or {}).get("items") or []
    return sum(int(it.get("views") or 0) for it in items)


def pageviews_between(name, start_date, end_date, max_langs=None):
    """全言語版Wikipediaの [start_date, end_date] のページビュー合計。任意期間版（日次収集＝現在窓／backfill＝発売前窓 の両方が使う）。
    Wikidata sitelinks で各言語の記事タイトルを得て、言語ごとに合算。公式API・キー不要。1言語だけの失敗はスキップ（合計は返す）。"""
    smap = _wd_sitelinks_map(name)
    if not smap:
        return 0
    cap = max_langs or MAX_PV_LANGS
    total = 0
    used = 0
    for key, title in smap.items():
        if used >= cap:
            break
        lang = key[:-4]                       # "enwiki" -> "en"、"zh_yuewiki" -> "zh_yue"
        if not lang:
            continue
        project = f"{lang.replace('_', '-')}.wikipedia"   # "zh_yue" -> "zh-yue.wikipedia"
        try:
            total += _pageviews_range(project, title, start_date, end_date)
        except (urllib.error.URLError, ValueError, OSError):
            pass                              # その言語版だけ諦めて続行（合計は返す）
        used += 1
        time.sleep(0.05)                      # 各言語版に優しく
    return total


def src_wikipedia_pageviews(name):
    """★全言語版Wikipediaの直近ページビュー合計＝地域に偏らない“実際の閲覧＝関心”の総量（日次収集・現在窓）。
    「世界中の記事をまんべんなく」の理想に最も近い“実閲覧”シグナル（件数ではなく人が読んだ回数）。公式API・キー不要。"""
    end = datetime.now(timezone.utc).date()
    return pageviews_between(name, end - timedelta(days=PAGEVIEW_DAYS), end)


def src_gdelt(name):
    """GDELT の直近1週間の**世界の多言語ニュース**記事数（65言語超・地域横断）。公式API・キー不要。壊れたら例外→スキップ。
    GDELT はクエリに敏感：フレーズ（空白入り）は引用符・単語はそのまま。sort等の余計な指定は外す。"""
    query = f'"{name}"' if " " in (name or "") else (name or "")
    q = urllib.parse.urlencode({"query": query, "mode": "artlist", "maxrecords": "250",
                                "timespan": "1w", "format": "json"})
    data = _get_json("https://api.gdeltproject.org/api/v2/doc/doc?" + q, timeout=30)  # GDELTは遅いので長め
    arts = (data or {}).get("articles") or []
    return len(arts)


SOURCE_FUNCS = {
    # 地域に偏らない“世界横断”系（理想に近い）
    "gdelt": src_gdelt,                       # 世界の多言語ニュース記事数
    "wikipedia_pageviews": src_wikipedia_pageviews,  # 全言語版Wikipediaの実閲覧合計＝世界の関心（最も強い横断シグナル・重い）
    "wikidata_sitelinks": src_wikidata_sitelinks,  # 何言語のWikipediaに記事があるか＝世界的知名度
    # 地域/コミュニティ寄り（補助・モデルが重みを判断）
    "wikipedia_en": src_wikipedia_en,         # 英語圏
    "wikipedia_ja": src_wikipedia_ja,         # 国内
    "hackernews": src_hackernews,             # 英語圏・技術者寄り（偏りあり）
    "youtube": src_youtube,                   # 世界だが要APIキー
    "note": src_note,                         # 既定外（403）
}


def _write(sql_fn):
    """書き込みを実行。Neon はアイドルで compute が scale-to-zero するため、最初の書き込みが
    復帰中の一時 read-only 窓（SQLSTATE 25006）に当たりやすい。指数バックオフで多め（既定6回・
    合計~75s）に再接続・再試行して cold start を吸収する。トランザクション失敗はロールバック＝二重書き込みなし。"""
    attempts = int(os.environ.get("WRITE_RETRIES") or "14")  # write-primary の cold-start が数分かかる時があるため長め（合計~8分）
    cap = int(os.environ.get("WRITE_BACKOFF_CAP") or "45")
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
            wait = min(cap, 3 * (2 ** i))
            print(f"[retry] 一時的な read-only/接続断（{getattr(e, 'pgcode', '')}）→ {wait}s 後に再試行 ({i+1}/{attempts})")
            time.sleep(wait)
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
    targets = get_targets()   # 先に READ して Neon compute を起こす（cold start の read-only 窓を短くする）
    if not DRY_RUN:
        ensure_table()        # その後に書き込み（テーブル作成）＝再試行つき。dry-run では触らない。
    active = [s for s in SOURCES if s in SOURCE_FUNCS and (s != "youtube" or YOUTUBE_API_KEY)]
    skipped = [s for s in SOURCES if s not in active]
    print(f"{'[dry-run] ' if DRY_RUN else ''}対象 {len(targets)} 件・ソース {active}"
          + (f"（未設定でスキップ: {skipped}）" if skipped else ""))
    if not targets or not active:
        if not active:
            print("有効なソースがありません（設定を確認）。何もしません。")
        return

    rows = []
    printed = 0
    print_cap = 10 ** 9 if DRY_RUN else 12   # dry-run は全件表示して各ソースの値を確認できるように
    for appid, name in targets:
        qname = _clean_name(name)   # 検索は正規化名で（一致率↑）。保存キーは appid のまま。
        for s in active:
            try:
                n = SOURCE_FUNCS[s](qname)
            except urllib.error.HTTPError as e:
                print(f"  [skip] {s:12} appid={appid} '{(name or '')[:24]}': HTTP {e.code}")
                n = None
            except (urllib.error.URLError, ValueError, TimeoutError, OSError) as e:
                print(f"  [skip] {s:12} appid={appid} '{(name or '')[:24]}': {type(e).__name__}")
                n = None
            if n is not None:
                rows.append((appid, s, int(n)))
                if printed < print_cap:
                    print(f"  {s:12} {appid:>9} {int(n):>9}  {(name or '')[:40]}")
                    printed += 1
            time.sleep(0.2)  # 軽い間隔（各APIに優しく）

    per_sum = {}
    for _, s, n in rows:
        per_sum[s] = per_sum.get(s, 0) + int(n)
    print(f"ソース別 合計カウント: {per_sum}")

    if DRY_RUN:
        print(f"[dry-run] 収集のみ・DB書き込みなし（{len(rows)} 行を記録せず）。")
        return
    if not rows:
        print("記録行 0（全ソースが0/失敗）。")
        return

    _write(lambda cur: execute_batch(
        cur, "INSERT INTO web_mentions (appid, source, mentions) VALUES (%s, %s, %s)",
        rows, page_size=500))
    print(f"記録: {len(rows)} 行")


if __name__ == "__main__":
    main()
