"""
②-appdetails: 発売日 / is_free / 価格・割引 / ジャンル / type / 開発元 / 販売元 / カテゴリ / DLC / サムネURL / 対応言語
  を全名簿ローリングで取得する。
⑱ header_image＝サムネの**正しい**URL。新Steamは …/store_item_assets/steam/apps/{appid}/{40桁ハッシュ}/header.jpg
  に画像を置き、ハッシュは推測できない＝URLを組み立てる方式では当たらない。appdetails の header_image が唯一の手段。
⑨-b supported_languages（原文）＋ has_japanese（日本語を含むか・NULL＝未取得）。⑱と同じ1回の取得で両方取れる。
③拡張(v2): 体験版(demo)を発見して名簿に登録し、親ゲームへ紐づける（発売前の早期人気シグナルの土台）。
  - 親ゲームの appdetails の `demos`[{appid,...}] から体験版appidを発見 → games に INSERT（status=active・親appidを fullgame_appid に）。
  - 体験版自身の appdetails の `fullgame.appid`（文字列）から親appidを取得 → 自分の行の fullgame_appid を設定。
  - fullgame_appid は COALESCE で更新＝一度ついた親リンクを後続のNULLで壊さない（非破壊）。
取得口 = store.steampowered.com/api/appdetails（ストアフロント・APIキー不要）。
  率制限 ≈ 200req/5分/IP・1コール1appid（一括不可）。安全側で控えめに叩く（既定 0.5/s）。
保存の分離:
  - 静的（発売日/is_free/ジャンル/type/fullgame_appid）→ games 表の列（一度取れば再取得は稀）。
  - 価格・割引（"腐る"）→ price_snapshots（時系列・review_snapshots と同型）。
対象 = ハイブリッド: 発売前(coming_soon)＋活動中＋監視を先に観て、残り枠で last_appdetails_check_at 古い順に広げる。
失敗時の扱い:
  - ネットワーク/429 → last_appdetails_check_at を進めない＝次回再試行（daily/review と同設計）。
  - success=false（販売終了・地域外） → 「確認済み」として時刻を進める（無限再試行を避ける）。
取りこぼし対策ガード(job_state): 直近 MIN_INTERVAL_HOURS 以内に成功してたらスキップ＝1日1回。
長時間対策: FLUSH_EVERY 件ごとに逐次保存（途中終了でも進捗が残る）。
"""
import os
import json
import time
import threading
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2.extras import execute_batch, Json

from _filters import is_adult_from_details

DATABASE_URL = os.environ["DATABASE_URL"]

APPDETAILS_CAP = int(os.environ.get("APPDETAILS_CAP")     or "6000")   # 1回の観測上限（暫定・率制限とtimeout内に収める）
ACTIVE_DAYS    = int(os.environ.get("ACTIVE_DAYS")        or "30")     # 「活動中」の直近日数（焦点と揃える・仮置き）
RATE_PER_SEC   = float(os.environ.get("APPDETAILS_RATE")  or "0.5")    # 約0.5/s = 150/5分（上限200/5分の安全側）
WORKERS        = int(os.environ.get("APPDETAILS_WORKERS") or "4")
CC             = os.environ.get("APPDETAILS_CC")          or "jp"      # 価格の地域（JPY）
LANG           = os.environ.get("APPDETAILS_LANG")       or "english"
FLUSH_EVERY    = int(os.environ.get("APPDETAILS_FLUSH")   or "500")    # この件数ごとに逐次保存
SAMPLE_LOG     = int(os.environ.get("APPDETAILS_SAMPLE_LOG") or "3")

JOB_NAME = "appdetails_sweep"
MIN_INTERVAL_HOURS = int(os.environ.get("MIN_INTERVAL_HOURS") or "20")  # 暫定値（環境変数で調整可）
FORCE = (os.environ.get("FORCE") or "").strip().lower() in ("1", "true", "yes")  # 手動で強制実行

API = "https://store.steampowered.com/api/appdetails"

_lock = threading.Lock()
_next = [0.0]
_interval = 1.0 / RATE_PER_SEC

_status = Counter()
_status_lock = threading.Lock()


def _throttle():
    with _lock:
        now = time.monotonic()
        if _next[0] > now:
            time.sleep(_next[0] - now)
            now = time.monotonic()
        _next[0] = now + _interval


def _note(code):
    with _status_lock:
        _status[code] += 1


# Steam の release_date.date は表示用の文字列（言語/地域で形が変わる）。
# best-effort で YYYY-MM-DD に。読めなければ None（＝推測しない。原文は別途 release_date_text に保存）。
_DATE_FORMATS = (
    "%d %b, %Y", "%d %b %Y", "%b %d, %Y", "%B %d, %Y",
    "%d %B %Y", "%b %Y", "%B %Y", "%Y",
)


def parse_release_date(text, coming_soon):
    if coming_soon or not text:
        return None
    t = text.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(t, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _to_int(v):
    """Steam は appid を int でも str でも返す（fullgame.appid は文字列）。失敗時は None。"""
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _nz(s):
    """空文字は None に寄せる（「取れていない」と「空」を混ぜない＝0802E-05）。"""
    if s is None:
        return None
    s = str(s).strip()
    return s or None


# ⑨-b 日本語対応の有無。supported_languages は **呼び出し元の言語で表記が変わる**（"Japanese" / "日本語"）。
#   このスクリプトは以前から l=english を明示しているので英語表記で返るはずだが、
#   表記が変わっても取りこぼさないよう両方を見る（どちらで来ても意味は同じ＝日本語対応）。
#   ★取れなかった作品は None（＝まだ調べていない）。False（＝日本語非対応）と必ず区別する（0802E-05）。
JA_MARKERS = ("Japanese", "日本語")


def has_japanese_from(text):
    t = _nz(text)
    if t is None:
        return None
    return any(m in t for m in JA_MARKERS)


def fetch_appdetails(appid):
    _throttle()
    url = API + "?" + urllib.parse.urlencode({"appids": appid, "cc": CC, "l": LANG})
    req = urllib.request.Request(url, headers={"User-Agent": "game-site-appdetails/0.1"})
    for _ in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.load(r)
            entry = (data or {}).get(str(appid)) or {}
            if not entry.get("success"):
                _note("nodata")
                return appid, "nodata", None
            d = entry.get("data") or {}
            rd = d.get("release_date") or {}
            rd_text = rd.get("date")
            coming = bool(rd.get("coming_soon"))
            po = d.get("price_overview")
            price = None
            if isinstance(po, dict):
                price = (po.get("currency"), po.get("initial"),
                         po.get("final"), po.get("discount_percent"))
            # ③ 体験版リンク: 自分が体験版なら親appid（fullgame.appid＝文字列）、親なら demos の体験版appid群。
            fg = d.get("fullgame")
            fullgame_appid = _to_int(fg.get("appid")) if isinstance(fg, dict) else None
            demo_appids = []
            for x in (d.get("demos") or []):
                if isinstance(x, dict):
                    da = _to_int(x.get("appid"))
                    if da is not None:
                        demo_appids.append(da)
            # ⑱ サムネの正しいURL。新しいSteamは .../store_item_assets/steam/apps/{appid}/{40桁のハッシュ}/header.jpg に
            #   画像を置いており、ハッシュは推測できない＝URLを組み立てる方式では当たらない。appdetails の header_image が唯一の手段。
            #   ★?t=… のクエリは落とさずそのまま保存する（Steam側でハッシュが変わったときの手掛かりになる）。
            sup_lang = _nz(d.get("supported_languages"))   # ⑨-b 原文をそのまま保存（捨てない）
            fields = {
                "name": d.get("name"),
                "header_image": _nz(d.get("header_image")),
                "supported_languages": sup_lang,
                "has_japanese": has_japanese_from(sup_lang),
                "release_date_text": rd_text,
                "release_date": parse_release_date(rd_text, coming),
                "coming_soon": coming,
                "is_free": bool(d.get("is_free")),
                "app_type": d.get("type"),
                "genres": d.get("genres") or [],
                "developers": d.get("developers") or [],
                "publishers": d.get("publishers") or [],
                "categories": d.get("categories") or [],
                "dlc": d.get("dlc") or [],
                "website": d.get("website"),   # L3: 公式website（appdetails basic に含まれる・null可）
                "is_adult": is_adult_from_details(d),   # 成人向け判定（content_descriptors {3,4}/ジャンル）＝サイトから除外用
                "price": price,
                "fullgame_appid": fullgame_appid,
                "demos": demo_appids,
            }
            _note("ok")
            return appid, "ok", fields
        except urllib.error.HTTPError as e:
            if e.code == 429:
                _note("429")
                time.sleep(min(int(e.headers.get("Retry-After", "60") or "60"), 120))
                continue
            _note("http_%d" % e.code)
            return appid, "fail", None
        except Exception:
            _note("error")
            time.sleep(2)
    _note("giveup")
    return appid, "fail", None


def should_run():
    """直近 MIN_INTERVAL_HOURS 時間以内に成功していれば False（=今日はもう回した）。
    FORCE 指定時は常に True。状態を確認できないときは安全側で True（収集を取りこぼさない＝fail-open）。"""
    if FORCE:
        print("[guard] FORCE 指定のためクールダウンを無視して実行します。")
        return True
    try:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT last_success_at, "
                    "       (last_success_at > now() - (%s * interval '1 hour')) AS too_soon "
                    "FROM job_state WHERE job = %s",
                    (MIN_INTERVAL_HOURS, JOB_NAME),
                )
                row = cur.fetchone()
        finally:
            conn.close()
    except Exception as e:
        print(f"[guard] 状態の確認に失敗（{e}）。安全側で実行します（fail-open）。")
        return True
    if row and row[0] is not None and row[1]:
        print(f"[guard] 直近 {MIN_INTERVAL_HOURS}h 以内に成功済み（last_success_at={row[0]}）。今回はスキップします。")
        return False
    return True


def mark_success():
    """正常完了を job_state に記録する（重複しても非破壊なので失敗しても安全）。"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO job_state (job, last_success_at) VALUES (%s, now()) "
                    "ON CONFLICT (job) DO UPDATE SET last_success_at = EXCLUDED.last_success_at",
                    (JOB_NAME,),
                )
        finally:
            conn.close()
        print(f"[guard] 成功を記録しました（job={JOB_NAME}）。")
    except Exception as e:
        print(f"[guard] 成功の記録に失敗（{e}）。次回は再実行されます。")


def get_targets():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT appid FROM games "
                "ORDER BY (CASE WHEN status = 'watchlist' "
                "               OR coming_soon IS TRUE "
                "               OR (last_active_at IS NOT NULL "
                "                   AND last_active_at >= now() - (%s * interval '1 day')) "
                "          THEN 0 ELSE 1 END) ASC, "
                "         last_appdetails_check_at ASC NULLS FIRST "
                "LIMIT %s",
                (ACTIVE_DAYS, APPDETAILS_CAP),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def flush(buffer):
    """buffer = [(appid, status, fields), ...] を1回ぶんDBへ書き、(静的更新件数, 体験版登録件数) を返す。"""
    static_rows = []   # (rdt, rd, cs, isf, atp, genres_json, fullgame_appid, appid) ※UPDATEのプレースホルダ順
    price_rows = []    # (appid, currency, initial, final, discount)
    demo_rows = []     # (demo_appid, name, parent_appid) ※体験版を名簿に登録＋親リンク
    seen_demo = set()
    for appid, status, f in buffer:
        if status == "ok":
            static_rows.append((f["release_date_text"], f["release_date"], f["coming_soon"],
                                f["is_free"], f["app_type"], Json(f["genres"]),
                                Json(f["developers"]), Json(f["publishers"]),
                                Json(f["categories"]), Json(f["dlc"]),
                                f.get("website"), f.get("is_adult"),
                                f.get("header_image"), f.get("supported_languages"),
                                f.get("has_japanese"),
                                f.get("fullgame_appid"), appid))
            if f["price"]:
                cur_, ini, fin, disc = f["price"]
                if all(x is not None for x in (cur_, ini, fin)):
                    price_rows.append((appid, cur_, ini, fin, disc))
            for da in (f.get("demos") or []):
                if da not in seen_demo:
                    seen_demo.add(da)
                    base = (f.get("name") or ("appid " + str(appid)))[:120]
                    demo_rows.append((da, base + " (Demo)", appid))
        elif status == "nodata":
            # 販売終了・地域外＝「確認したが中身が無い」。他の静的列と同じ扱いで NULL に戻す
            # （＝空文字は入れない。has_japanese も NULL＝不明であって false ではない）。
            static_rows.append((None,) * 16 + (appid,))
        # "fail" は何も書かない（last_appdetails_check_at を進めない＝次回再試行）
    if not static_rows and not price_rows and not demo_rows:
        return 0, 0
    # Neon のサーバーレス compute 復帰／オートスケール遷移時、接続が一時的に read-only 窓へ当たり
    # "cannot execute UPDATE in a read-only transaction"（SQLSTATE 25006）や接続断が起きうる（数秒で解消する一過性）。
    # 指数バックオフで数回だけ再接続・再試行する。成功時は素通り。恒久的な権限/構文エラー等は即送出。
    # トランザクション（with conn）は失敗時ロールバックされるため、再試行しても二重書き込みにならない。
    attempts = int(os.environ.get("WRITE_RETRIES") or "4")
    for i in range(attempts):
        conn = None
        try:
            conn = psycopg2.connect(DATABASE_URL)
            with conn, conn.cursor() as cur:
                if static_rows:
                    execute_batch(
                        cur,
                        "UPDATE games SET "
                        "  release_date_text = %s, release_date = %s, coming_soon = %s, "
                        "  is_free = %s, app_type = %s, genres = %s, "
                        "  developers = %s, publishers = %s, categories = %s, dlc = %s, "
                        "  website = %s, is_adult = COALESCE(%s, is_adult), "
                        "  header_image = %s, supported_languages = %s, has_japanese = %s, "
                        "  fullgame_appid = COALESCE(%s, fullgame_appid), "
                        "  last_appdetails_check_at = now() "
                        "WHERE appid = %s",
                        static_rows, page_size=500,
                    )
                if price_rows:
                    execute_batch(
                        cur,
                        "INSERT INTO price_snapshots (appid, currency, initial, final, discount_percent) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        price_rows, page_size=500,
                    )
                if demo_rows:
                    # 体験版を名簿に登録（active＝以後 daily/dense がCCUを採取）。既存行は親リンクのみ補完（非破壊）。
                    execute_batch(
                        cur,
                        "INSERT INTO games (appid, name, status, fullgame_appid) "
                        "VALUES (%s, %s, 'active', %s) "
                        "ON CONFLICT (appid) DO UPDATE SET "
                        "  fullgame_appid = COALESCE(games.fullgame_appid, EXCLUDED.fullgame_appid)",
                        demo_rows, page_size=500,
                    )
            return len(static_rows), len(demo_rows)
        except psycopg2.Error as e:
            code = getattr(e, "pgcode", None)
            transient = isinstance(e, psycopg2.OperationalError) or code == "25006"  # 25006 = read_only_sql_transaction
            if not transient or i == attempts - 1:
                raise
            wait = 3 * (2 ** i)  # 3, 6, 12 秒
            print(f"  [retry] 一時的に書き込み不可（{type(e).__name__} pgcode={code}）→ {wait}s 後に再接続・再試行 ({i + 1}/{attempts})")
            time.sleep(wait)
        finally:
            if conn is not None:
                conn.close()


def ensure_schema():
    """新設列を冪等に用意（既存なら何もしない）。is_adult＝成人向け判定（サイトから除外用）。
    ⑱ header_image＝appdetails が返すサムネの正しいURL（ハッシュ入りで推測不可）。
    ⑨-b supported_languages＝対応言語の原文／has_japanese＝そこに日本語が含まれるか（NULL＝未取得）。"""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS is_adult boolean")
            cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS header_image text")
            cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS supported_languages text")
            cur.execute("ALTER TABLE games ADD COLUMN IF NOT EXISTS has_japanese boolean")
    finally:
        conn.close()


def report_coverage():
    """★合格条件の“原因の指標”をログに出す：収集が効いたかを列の埋まり具合で測る。
    件数（結果）ではなく「NULL でない行の割合」を見る＝母数も必ず併記する。"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*), count(header_image), count(supported_languages), "
                    "       count(*) FILTER (WHERE has_japanese IS TRUE), "
                    "       count(*) FILTER (WHERE has_japanese IS FALSE), "
                    "       count(*) FILTER (WHERE has_japanese IS NULL), "
                    "       count(*) FILTER (WHERE last_appdetails_check_at IS NOT NULL) "
                    "FROM games"
                )
                n, n_img, n_lang, ja_t, ja_f, ja_n, n_enriched = cur.fetchone()
        finally:
            conn.close()
    except Exception as e:
        print(f"[coverage] 集計に失敗（{e}）。収集自体には影響しません。")
        return
    def pct(x, base):
        return f"{(100.0 * x / base):.1f}%" if base else "—"
    print(f"[coverage] games 総数 {n} 件（うち appdetails 済み {n_enriched} 件）")
    print(f"  header_image が NULL でない: {n_img} 件 = 全体の {pct(n_img, n)} / appdetails済みの {pct(n_img, n_enriched)}")
    print(f"  supported_languages が NULL でない: {n_lang} 件 = 全体の {pct(n_lang, n)}")
    print(f"  has_japanese: true={ja_t} / false={ja_f} / NULL(未取得)={ja_n}")


def main():
    ensure_schema()            # クールダウンでスキップする場合でも列だけは必ず用意（エクスポートが参照するため）
    if not should_run():
        return
    targets = get_targets()
    # l（表示言語）を必ずログに残す：supported_languages の表記はこれで決まる（"Japanese" か「日本語」か）。
    print(f"今回appdetailsを観測する対象: {len(targets)} 件 "
          f"(cap={APPDETAILS_CAP}, rate={RATE_PER_SEC}/s, workers={WORKERS}, cc={CC}, l={LANG}, flush={FLUSH_EVERY})")

    buffer = []
    written = 0
    demos_total = 0
    shown = 0
    counts = Counter()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for appid, status, f in ex.map(fetch_appdetails, targets):
            counts[status] += 1
            buffer.append((appid, status, f))
            if status == "ok" and shown < SAMPLE_LOG:
                print(f"  sample appid={appid}: type={f['app_type']} is_free={f['is_free']} "
                      f"release='{f['release_date_text']}'(parsed={f['release_date']}) "
                      f"price={f['price']} demos={f.get('demos')} fullgame={f.get('fullgame_appid')} "
                      f"genres={[g.get('description') for g in f['genres']]} website={f.get('website')}")
                # ⑱⑨-b の実測サンプル：genres/release が英語のままか（l=english が効いているか）も
                # 上の行と並べて読めるようにする。supported_languages は長いので先頭だけ。
                print(f"    header_image={f.get('header_image')}")
                print(f"    has_japanese={f.get('has_japanese')} languages={(f.get('supported_languages') or '')[:120]!r}")
                shown += 1
            if len(buffer) >= FLUSH_EVERY:
                w, dms = flush(buffer)
                written += w
                demos_total += dms
                buffer = []
                print(f"  …逐次保存: 累計 {written} 件 / 体験版登録 {demos_total} 件"
                      f"（ok={counts['ok']} nodata={counts['nodata']} fail={counts['fail']}）")
    if buffer:
        w, dms = flush(buffer)
        written += w
        demos_total += dms

    print(f"取得: ok(データ有)={counts['ok']} / nodata(販売終了等)={counts['nodata']} / fail(再試行)={counts['fail']}")
    print("ステータス内訳:", dict(_status))
    print(f"保存（last_appdetails_check_at 更新）: {written} 件。体験版を発見・登録: {demos_total} 件。")
    report_coverage()   # ⑱⑨-b：列の埋まり具合＝収集が効いたかの指標
    mark_success()


if __name__ == "__main__":
    main()
