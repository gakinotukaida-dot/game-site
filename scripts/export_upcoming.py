"""
表示用エクスポート（候補4「これから来そう」＝発売前・羽根予想つき）── 2026-07-07 / v3
================================================================
役割：Neon を「読むだけ」で発売前ゲームを拾い、**羽根予想（跳ね確率）** を付けて data/upcoming.json に書き出す。

★このサイトの核＝羽根予想：複数の実測シグナルを **自社の過去実績で較正したモデル（prelaunch_model.json）** で
  総合し、各作品の「発売直後に跳ねる確率」を出す。当てずっぽうではなく、外れうることは明示（参考にとどめる）。
  使うシグナル（すべて自前観測・ToUクリーン・as-of）：
    体験版CCU / Twitch視聴者 / 配信者数 / 告知数 / 開発元の実績(過去最高CCU・最大レビュー) / ジャンル命中率 / 無料か
  → prelaunch_features.py に定義を集約（学習と推論で同一）。

線：DBは読み取り専用（SELECTのみ）。書き込みは data/upcoming.json 1ファイルのみ・毎回上書き＝可逆。新規収集ゼロ。
    著作物は載せない（ジャンル語・appid・数値のみ）。モデルが無ければ確率は出さず従来の期待度に自動フォールバック。
env：GENRE_MAX / LIMIT / MODEL_PATH / UPCOMING_APP_TYPES。
"""

import json
import os
from datetime import datetime, date

import psycopg2

import prelaunch_features as F
from _filters import not_adult
from _market import fetch_market, market_of

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/upcoming.json"
MODEL_PATH = os.environ.get("MODEL_PATH") or "data/prelaunch_model.json"

GENRE_MAX = int(os.environ.get("GENRE_MAX") or "7")
LIMIT = int(os.environ.get("LIMIT") or "200")
# 発売済み（居座り）を落とすと表示枠が減るので、多めに取得してから絞る（絞った後に LIMIT まで）。
OVERFETCH = float(os.environ.get("UPCOMING_OVERFETCH") or "1.6")

# ⑰ 付随物（DLC・サントラ・壁紙・バンドル等）を「これから来そう」から除く。
#   使うのは games.app_type＝Steam appdetails の `type`（appdetails_sweep.py が収集済み＝新規収集ゼロ）。
#   ★名前による除外（"DLC" を含む等）は入れない：誤除外のリスクが高く、一次情報の列がある以上、代理指標を使う理由がない。
#   ★app_type の実値（'game' か 'Game' か等）はこの環境からDBに繋げず未確認なので、
#     ①比較は小文字化して行い、②許可リストは env（UPCOMING_APP_TYPES・既定 "game"）で判定を待たずに変えられるようにする。
APP_TYPES = [t.strip().lower() for t in (os.environ.get("UPCOMING_APP_TYPES") or "game").split(",") if t.strip()]


def is_game(alias="g"):
    """games（別名 alias）から付随物（dlc/music/demo…）を除外する WHERE 断片。
    NULL 安全：app_type IS NULL は通す。理由＝app_type は appdetails でエンリッチ済みの作品にしか付かず、
    落とすと「見つかったばかりの新作」ほど巻き添えになる（＝このサイトが最も拾いたいもの）。
    `_filters.not_adult` と同じ「既知の該当だけ落とす」2層の考え方。
    UPCOMING_APP_TYPES="" ＝許可リスト空＝この除外を丸ごと無効化（判定を待たずに戻せる逃げ道）。"""
    if not APP_TYPES:
        return "TRUE"
    return "(" + alias + ".app_type IS NULL OR lower(" + alias + ".app_type) = ANY(%(app_types)s))"


# ★「これから来そう」に載せる条件（＝まだ発売前）。
#   - coming_soon フラグ、または未来の確定発売日。
#   - ただし発売日が確定していて過去なら除外（coming_soon が古いままでも居座らせない）。
#     ※ appdetails 再取得の遅れで coming_soon=true が残る“居座り”は、列が NULL のことが多い。
#       その取りこぼしは compute_rows の _is_released（release_date_text の日付判定）で拾う。
#   - 成人向けは除外。
UPCOMING_BASE_WHERE = ("(g.coming_soon IS TRUE OR (g.release_date IS NOT NULL AND g.release_date > now()::date))"
                       " AND (g.release_date IS NULL OR g.release_date > now()::date)"
                       " AND " + not_adult("g"))
# ⑰ ＋ゲーム本体だけ（付随物を除く）。戻すときはこの1行を UPCOMING_BASE_WHERE に戻すか、env で許可リストを広げる。
UPCOMING_WHERE = UPCOMING_BASE_WHERE + " AND " + is_game("g")

# 診断：発売前の母集団に app_type が実際どう入っているかを1回だけ数える（読み取りのみ・ログに出すだけ）。
# ★これが「原因の指標」＝除外条件が効くかは app_type の実値で決まる。件数（結果）ではなく分布を見る。
APP_TYPE_DIST_SQL = f"""
SELECT COALESCE(g.app_type, '(NULL)') AS t, count(*) AS n
FROM games g
WHERE {UPCOMING_BASE_WHERE}
GROUP BY 1 ORDER BY 2 DESC
"""


def build_query(web_ok):
    return f"""
WITH {F.cte_prelude()},
self_up AS (
  SELECT g.appid FROM games g WHERE {UPCOMING_WHERE}
),
{F.dev_best_cte('self_up', 'now()')}
SELECT g.appid, g.name, g.release_date, g.release_date_text, g.genres, g.coming_soon,
  {F.feature_sql(asof='now()', web_ok=web_ok)},
  db.dev_best_peak, db.dev_best_reviews
FROM games g
LEFT JOIN dev_best db ON db.appid = g.appid
WHERE {UPCOMING_WHERE}
ORDER BY (g.release_date IS NULL), g.release_date ASC NULLS LAST, g.name ASC
LIMIT %(limit)s
"""


def _descs(arr, cap):
    if not isinstance(arr, list):
        return []
    out = []
    for x in arr:
        if isinstance(x, dict):
            d = x.get("description")
            if d and str(d).strip():
                out.append(str(d).strip())
        if len(out) >= cap:
            break
    return out


def _release_iso(rd, text):
    if rd is None:
        return text
    try:
        return rd.isoformat()
    except AttributeError:
        return str(rd)


# Steam の release_date.date（表示用文字列）を粒度つきで解釈するための書式。
# 日付まで揃うもの／月まで／年だけ、をそれぞれ判別する（粗い表記の過剰除外を防ぐため）。
_REL_DATE_FORMATS = (
    ("%d %b, %Y", "day"), ("%d %b %Y", "day"), ("%b %d, %Y", "day"), ("%B %d, %Y", "day"),
    ("%d %B %Y", "day"), ("%b %Y", "month"), ("%B %Y", "month"), ("%Y", "year"),
)


def _parse_release_period(text):
    """release_date_text を (precision, year, month, day) に。precision は 'day'/'month'/'year'。
    読めなければ None（＝発売済みとは判定しない＝安全側で残す）。
    ※ coming_soon の表示日は「2026」「Jul 2026」のように粒度が粗いことがあるので、
      粒度を保って“期間が丸ごと過去のときだけ発売済み”と扱う（過剰除外を避ける）。"""
    if not text:
        return None
    t = str(text).strip()
    for fmt, prec in _REL_DATE_FORMATS:
        try:
            dt = datetime.strptime(t, fmt)
        except ValueError:
            continue
        if prec == "year":
            return ("year", dt.year, None, None)
        if prec == "month":
            return ("month", dt.year, dt.month, None)
        return ("day", dt.year, dt.month, dt.day)
    return None


def _is_released(release_date, release_date_text, today):
    """今日時点で「もう発売済み」なら True（＝これから来そうから除外）。判定は保守的に：
      1) release_date 列が確定していて今日以前（coming_soon が古くても発売済みは落とす）。
      2) 列が無くても release_date_text が“日付まで揃った”表記で今日以前
         （appdetails 再取得の遅れで coming_soon=true が残る“居座り”対策）。
      3) 月だけ/年だけの粗い表記は、その期間が丸ごと過去のときだけ発売済み扱い（過剰除外を避ける）。"""
    if release_date is not None:
        try:
            if release_date <= today:
                return True
        except TypeError:
            pass
    p = _parse_release_period(release_date_text)
    if not p:
        return False
    prec, y, m, d = p
    if prec == "day":
        return date(y, m, d) <= today
    if prec == "month":
        return (y, m) < (today.year, today.month)
    if prec == "year":
        return y < today.year
    return False


def _load_model():
    try:
        with open(MODEL_PATH, encoding="utf-8") as f:
            m = json.load(f)
        if m.get("base_rate") and m.get("woe"):
            return m
    except (OSError, ValueError):
        pass
    return None


def _iv(v):
    return int(v) if v is not None else None


def compute_rows(conn, limit=None):
    """発売前ゲームを読み、各作品の羽根予想（spike_prob/expect/conf/factors…）を付けた行リストを返す。
    ※ 予測の“単一の源”：これを export（表示）と prediction_log（記録）の両方が使う＝表示と記録の値が必ず一致（skew防止）。
       conn のセッション（readonly 等）は呼び出し側が設定する。並べ替え・payload化は呼び出し側の責務。
    返り値: (rows, model, base, validated)"""
    eff_limit = limit if limit is not None else LIMIT
    # 発売済み（居座り）を _is_released で落とすと枠が減るので、多めに取得してから絞る。
    fetch_limit = max(eff_limit + 50, int(eff_limit * OVERFETCH))
    with conn.cursor() as cur:
        cur.execute("SELECT now()::date")   # SQL の now()::date と同一基準の“今日”（発売済み判定に使う）
        today = cur.fetchone()[0]
        web_ok = F.web_mentions_exists(cur)   # web_mentions が無ければ web_* は NULL（無影響）
        cur.execute(build_query(web_ok), {"limit": fetch_limit, "app_types": APP_TYPES})
        cols = [d[0] for d in cur.description]
        recs = cur.fetchall()
        # ⑨-a：価格・割引・レビュー好評率。発売前なので多くは未取得（＝None）だが、
        # 予約価格やセール、体験版のレビューが付いている作品はここで拾える。
        _ai = cols.index("appid") if "appid" in cols else None
        mkt = fetch_market(cur, [r[_ai] for r in recs]) if _ai is not None else {}

    model = _load_model()
    base = (model.get("base_rate") if model else None) or 0.03
    validated = bool(model and model.get("readiness") == "validated")

    rows = []
    for rec in recs:
        d = dict(zip(cols, rec))
        # ★発売済み（＝もう「これから来そう」ではない）はここで除外。
        #   coming_soon フラグが古いままでも、確定日 or 表示日が今日以前なら落とす（居座り対策）。
        if _is_released(d.get("release_date"), d.get("release_date_text"), today):
            continue
        genres = _descs(d.get("genres"), GENRE_MAX)
        sqlvals = {k: d.get(k) for k in F.SQL_FEATURES}
        news_count = _iv(d.get("news_count")) or 0
        has_news = news_count > 0

        spike_prob = None
        factors = []
        active = 0
        if model:
            s = F.score(model, sqlvals, genres)
            spike_prob = round(s["prob"], 4)
            active = s["active"]
            # 上位の押し上げ要因（跳ねを上げているシグナル）を数個
            factors = [{"name": f["name"], "dir": f["dir"], "bucket": f["bucket"]}
                       for f in s["factors"] if f["dir"] == "up"][:3]

        # 期待度（表示用の粗い3段）＝確率が基準の何倍か。モデルが無ければ実測シグナルの有無で代替。
        if spike_prob is not None and base > 0:
            ratio = spike_prob / base
            expect = "high" if ratio >= 3 else "mid" if ratio >= 1.5 else "low"
        else:
            dc = _iv(d.get("demo_ccu"))
            expect = "high" if (dc and dc >= 50) else "mid" if ((dc and dc > 0) or has_news) else "low"

        # この予測の確からしさ（材料の量×モデルの検証強度）
        if spike_prob is None:
            conf = "na"
        elif validated and active >= 2:
            conf = "high"
        elif active >= 1:
            conf = "mid"
        else:
            conf = "low"

        rows.append({
            "appid": d.get("appid"),
            "name": d.get("name"),
            "release": _release_iso(d.get("release_date"), d.get("release_date_text")),
            "release_known": d.get("release_date") is not None,
            "coming_soon": bool(d.get("coming_soon")),
            "spike_prob": spike_prob,        # ★羽根予想＝跳ね確率（0..1）。モデル無しは null。
            "expect": expect,                # 粗い3段（high/mid/low）
            "conf": conf,                    # 予測の確からしさ（high/mid/low/na）
            "factors": factors,             # 押し上げ要因（name/dir/bucket）
            "active_signals": active,
            # 実測シグナルの生値（表示・監査用）
            "demo_ccu": _iv(d.get("demo_ccu")),
            "twitch_peak": _iv(d.get("twitch_peak")),
            "streamers": _iv(d.get("streamers")),
            "news_count": news_count,
            "has_news": has_news,
            "dev_best_peak": _iv(d.get("dev_best_peak")),
            "dev_best_reviews": _iv(d.get("dev_best_reviews")),
            "web_news": _iv(d.get("web_news")),       # 世界の多言語ニュース記事数（GDELT・最新）
            "web_views": _iv(d.get("web_views")),     # 全言語版Wikipediaの直近ページビュー合計（実閲覧・最新）
            "web_reach": _iv(d.get("web_reach")),     # 言語版Wikipediaの数（Wikidata・最新）
            "is_free": bool(d.get("is_free")),
            "genres": genres,
            # ⑨-a：未取得は None（0円・好評率0% と読めてはいけない＝0802E-05）。
            "price": market_of(mkt, d.get("appid"))["price"],
            "review": market_of(mkt, d.get("appid"))["review"],
        })
        if len(rows) >= eff_limit:   # 発売済みを除いた“発売前のみ”で LIMIT 件に達したら打ち切り
            break

    return rows, model, base, validated


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        rows, model, base, validated = compute_rows(conn)
        # ⑰ の診断：発売前の母集団の app_type 分布（＝除外条件が効くかを決める“原因の指標”）をログに出す。
        with conn.cursor() as cur:
            cur.execute(APP_TYPE_DIST_SQL)
            app_type_dist = cur.fetchall()
    finally:
        conn.close()

    # ★羽根予想の高い順に並べる（核＝跳ねそうな作品を上に）。モデル無しは expect→発売日で代替。
    _rank = {"high": 0, "mid": 1, "low": 2}
    rows.sort(key=lambda r: (
        -(r["spike_prob"] if r["spike_prob"] is not None else -1),
        _rank.get(r["expect"], 3),
        not r["release_known"],
    ))

    payload = {
        "view": "upcoming",
        "schema": "upcoming_v3",
        "source": "games(coming_soon/release_date) + 実測シグナル(体験版/Twitch/告知/開発元実績/ジャンル) + prelaunch_model",
        "note": ("発売前の羽根予想。spike_prob=跳ね確率＝自社実績で較正したモデルの出力（参考・外れうる）。"
                 "モデルが無い場合は expect のみ（実測シグナルの有無）。"
                 "発売日が今日以前の作品は除外（coming_soon フラグが古い“居座り”も落とす）。"),
        "generated_at": datetime.now().astimezone().isoformat(),
        "model": ({"schema": model.get("schema"), "readiness": model.get("readiness"),
                   "base_rate": model.get("base_rate"), "n_pairs": model.get("n_pairs"),
                   "hit_threshold": (model.get("params") or {}).get("hit_threshold"),
                   "validation_oos": model.get("validation_oos"),
                   "generated_at": model.get("generated_at")} if model else None),
        "params": {"limit": LIMIT, "genre_max": GENRE_MAX, "app_types": APP_TYPES},
        "count": len(rows),
        "rows": rows,
    }

    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")

    # ⑰ 診断ログ：app_type の実測分布と、許可リスト／未エンリッチ（NULL）の件数。
    #    ★母数（rows 総数）と必ず並べて読むこと（割合の意味が母数で変わるため）。
    n_all = sum(int(n) for _t, n in app_type_dist)
    n_null = sum(int(n) for t, n in app_type_dist if t == "(NULL)")
    n_kept = sum(int(n) for t, n in app_type_dist if t != "(NULL)" and str(t).lower() in APP_TYPES)
    print(f"app_type 分布（発売前の母集団 {n_all} 件・除外前）: "
          + " / ".join(f"{t}={n}" for t, n in app_type_dist))
    print(f"  許可リスト={APP_TYPES}（env UPCOMING_APP_TYPES）"
          f" → 通す {n_kept + n_null} 件（うち未エンリッチ NULL {n_null} 件）"
          f" / 落とす {n_all - n_kept - n_null} 件")

    n_news = sum(1 for r in rows if r["has_news"])
    n_high = sum(1 for r in rows if r["expect"] == "high")
    n_nogenre = sum(1 for r in rows if not r["genres"])
    print(f"書き出し: {OUT_PATH}（{len(rows)} 件・発売前・ジャンル無し {n_nogenre} 件）"
          f" model={'あり('+str(model.get('readiness'))+')' if model else 'なし'}"
          f" 期待度高 {n_high} / 最近告知 {n_news}")
    for r in rows[:10]:
        p = f"{r['spike_prob']*100:.1f}%" if r["spike_prob"] is not None else "—"
        drv = ",".join(f["name"] for f in r["factors"])
        print(f"  跳ね {p:<6} [{r['expect']:<4}/{r['conf']:<4}] {str(r['appid']):<8} "
              f"{(r['name'] or '')[:22]:<22} demo={r['demo_ccu']} tw={r['twitch_peak']} "
              f"devbest={r['dev_best_peak']} 要因={drv}")
    if not rows:
        print("該当0件＝発売前ゲームが games に無い（正常なこともある）。")


if __name__ == "__main__":
    main()
