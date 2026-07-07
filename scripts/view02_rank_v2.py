"""
view02 v2 ランキング（A＋B1）── 読み取り専用・Twitchデータ保存なし ── 2026-06-08
================================================================
v1 からの変更（検証済み）:
  A  : 現在値を「最新1点」→「直近6hの高分位(p90)」に（鋭さを残しつつ単発ノイズで決めない）。
       並びに「有意性(z)をソフトに考慮」（大型の小ブレが上位を独占しないように。硬い足切りはしない）。
       is_riser / is_launch は dense_sweep と同一定義（整合）。
  B1 : Twitch の「少数配信×高視聴」＝配信者による発掘の署名を、上限付きブースト＋ラベルで付与。
       集計値（ゲーム単位の視聴者数・配信数）のみ・**保存しない**（計算時取得→使い捨て）。

重み付けの思想（確定済み）: 手作り重みは「暫定の足場」。原因は順位を作るのでなく**上限付きで控えめに補正**する
  （存在しない伸びを捏造しない）。本番は案2でデータから学習予定。ここでの係数はすべて暫定・env可変。

安全性: 自前DBは read-only 固定・SELECTのみ。Twitchは集計のみ・保存なし・印字のみ。鍵はSecrets（オーナー）。
  Twitch鍵が無ければ B1 を自動スキップし、A 単独で動く（壊れない）。
"""

import os
import re
import json
import time
import datetime
import urllib.parse
import urllib.request

import psycopg2

from _filters import not_adult

DATABASE_URL = os.environ["DATABASE_URL"]
CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

# --- A: サージ ---
BASE_DAYS   = int(os.environ.get("BASE_DAYS")   or "14")
GAP_DAYS    = int(os.environ.get("GAP_DAYS")    or "1")
RECENT_HOURS = int(os.environ.get("RECENT_HOURS") or "6")
RECENT_Q    = float(os.environ.get("RECENT_Q")  or "0.9")   # 直近窓の高分位（鋭さ）
N0          = float(os.environ.get("N0")        or "10")
MIN_CURRENT = int(os.environ.get("MIN_CURRENT") or "100")
MIN_POINTS  = int(os.environ.get("MIN_POINTS")  or "5")
TOP_N       = int(os.environ.get("TOP_N")       or "30")
GENRE_MAX   = int(os.environ.get("GENRE_MAX")   or "6")   # 「どんなゲームか」タグ（games.genres 由来）
CATEGORY_MAX = int(os.environ.get("CATEGORY_MAX") or "6") # 補助タグ（games.categories 由来）
OUT_PATH    = os.environ.get("OUT_PATH") or "data/view02_rising.json"  # 出力JSON（独立・上書き・可逆）
CAND_N      = int(os.environ.get("CAND_N")      or "45")    # Twitch照会する候補数（base順上位）
# 有意性ソフト化
Z_REF       = float(os.environ.get("Z_REF")     or "3")
SIG_FLOOR   = float(os.environ.get("SIG_FLOOR") or "0.3")   # z=0でも残す重み（硬い足切り回避）
# dense整合（riser/launch）
RISER_MULT  = float(os.environ.get("RISER_MULT") or "3")
RISER_MIN_BASE = int(os.environ.get("RISER_MIN_BASE_OBS") or "2")
RISER_ABS_ADD = int(os.environ.get("RISER_ABS_ADD") or "200")
RISER_ABS_FLOOR = int(os.environ.get("RISER_ABS_FLOOR") or "200")
RISER_BASE_DAYS = int(os.environ.get("RISER_BASE_DAYS") or "14")
LAUNCH_DAYS = int(os.environ.get("LAUNCH_DAYS") or "14")
# きっかけ
NEWS_DAYS   = int(os.environ.get("CAUSE_NEWS_DAYS") or "7")
JP_NEWS_DAYS = int(os.environ.get("JP_NEWS_DAYS") or "7")   # C3: 国内話題（jp_news）の窓
JP_MIN_NAME = int(os.environ.get("JP_MIN_NAME") or "5")     # C3: 誤マッチ回避＝この文字数未満の名前は突合しない
REV_DAYS    = int(os.environ.get("CAUSE_REV_DAYS")  or "7")
REV_SURGE   = int(os.environ.get("CAUSE_REV_SURGE") or "50")
# レビュー急増の判定方式（休眠導入・既定 abs ＝現行と完全同一挙動）
#   abs      : 直近 REV_DAYS 日の総レビュー増が REV_SURGE 件以上で点灯（現行）。
#   relative : 「自分比」。今週増 ÷ そのゲームの平常週ペース ≥ REV_REL_MULT かつ 今週増 ≥ REV_ABS_FLOOR。
#              平常週ペース＝基準窓(REV_BASELINE_DAYS 日・今週より前)の増加 ÷ 週数。
#              平常を測る履歴が無いゲームは点灯しない（誤ラベルより無ラベルを優先）。
#   ★有効化条件：注目ゲームに (REV_BASELINE_DAYS + REV_DAYS) 日ぶんの履歴が貯まってから。
#     先に diagnose_review_history.py を短い基準窓で回して分布を確認→しきい値確定→ここを relative に。
REV_SURGE_MODE    = (os.environ.get("REV_SURGE_MODE") or "abs").strip().lower()
REV_BASELINE_DAYS = int(os.environ.get("REV_BASELINE_DAYS") or "14")  # relative時の平常窓（14=2週=曜日効果を相殺）
REV_REL_MULT      = float(os.environ.get("REV_REL_MULT")    or "3")   # 平常週ペースの何倍で急増とみなすか
REV_ABS_FLOOR     = int(os.environ.get("REV_ABS_FLOOR")     or "15")  # 極小ゲームのノイズ除け（最低増加数）
# 上限付きブースト（暫定の足場・案2で学習予定）
BOOST_SALE   = float(os.environ.get("BOOST_SALE")   or "0.05")
BOOST_NEWS   = float(os.environ.get("BOOST_NEWS")   or "0.05")
BOOST_JP     = float(os.environ.get("BOOST_JP")     or "0.03")   # C3: 国内話題（弱め・上限内）
BOOST_LAUNCH = float(os.environ.get("BOOST_LAUNCH") or "0.05")
BOOST_FREE   = float(os.environ.get("BOOST_FREE")   or "0.05")
BOOST_REVIEW = float(os.environ.get("BOOST_REVIEW") or "0.05")
B1_DISCOVERY = float(os.environ.get("B1_DISCOVERY") or "0.10")  # 少数配信×高視聴の発掘
B1_ATTENTION = float(os.environ.get("B1_ATTENTION") or "0.05")  # 配信注目
BOOST_CAP    = float(os.environ.get("BOOST_CAP")    or "0.30")  # 合計ブーストの上限
# B1 しきい値（暫定）
B1_FEW_CH    = int(os.environ.get("B1_FEW_CH")    or "10")   # 「少数配信」の上限
B1_CONC_REF  = float(os.environ.get("B1_CONC_REF") or "300") # 視聴/配信 の基準（集中度）
B1_VPC_REF   = float(os.environ.get("B1_VPC_REF")  or "0.3") # 視聴/CCU の基準（配信注目）

PARAMS = {"gap_days": GAP_DAYS, "base_days": BASE_DAYS, "recent_h": RECENT_HOURS,
          "recent_q": RECENT_Q, "n0": N0, "min_current": MIN_CURRENT,
          "min_points": MIN_POINTS, "cand_n": CAND_N, "riser_base_days": RISER_BASE_DAYS,
          "riser_min_base": RISER_MIN_BASE, "riser_mult": RISER_MULT,
          "riser_abs_add": RISER_ABS_ADD, "riser_abs_floor": RISER_ABS_FLOOR,
          "launch_days": LAUNCH_DAYS}

Q_MAIN = """
WITH latest AS (
  SELECT DISTINCT ON (appid) appid, player_count AS current_ccu, recorded_at AS last_at
  FROM player_counts ORDER BY appid, recorded_at DESC
),
recent6 AS (
  SELECT appid, percentile_cont(%(recent_q)s) WITHIN GROUP (ORDER BY player_count) AS recent_q, count(*) AS rn
  FROM player_counts WHERE recorded_at >= now() - make_interval(hours => %(recent_h)s)
  GROUP BY appid
),
base AS (
  SELECT appid,
    percentile_cont(0.5)  WITHIN GROUP (ORDER BY player_count) AS baseline,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY player_count) AS q1,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY player_count) AS q3,
    count(*) AS n_points
  FROM player_counts
  WHERE recorded_at <  now() - make_interval(days => %(gap_days)s)
    AND recorded_at >= now() - make_interval(days => %(base_days)s)
  GROUP BY appid
),
win AS (
  SELECT appid,
    max(player_count) FILTER (WHERE recorded_at >= now() - make_interval(hours => %(recent_h)s)) AS recent_max,
    avg(player_count) FILTER (WHERE recorded_at <  now() - make_interval(hours => %(recent_h)s)) AS base_avg,
    count(*)          FILTER (WHERE recorded_at <  now() - make_interval(hours => %(recent_h)s)) AS base_n
  FROM player_counts WHERE recorded_at >= now() - make_interval(days => %(riser_base_days)s)
  GROUP BY appid
)
SELECT l.appid, g.name, g.release_date, g.coming_soon, g.genres, g.categories,
  l.current_ccu,
  COALESCE(r.recent_q, l.current_ccu)                               AS recent_value,
  b.baseline, b.n_points,
  COALESCE(r.recent_q, l.current_ccu)::float / NULLIF(b.baseline,0) AS raw_ratio,
  (COALESCE(r.recent_q, l.current_ccu) - b.baseline) / NULLIF(b.q3 - b.q1, 0) AS robust_z,
  (w.recent_max IS NOT NULL AND w.base_avg IS NOT NULL AND w.base_n >= %(riser_min_base)s
    AND w.recent_max >= GREATEST(w.base_avg * %(riser_mult)s, w.base_avg + %(riser_abs_add)s)
    AND w.recent_max >= %(riser_abs_floor)s)                        AS is_riser,
  (g.coming_soon IS FALSE AND g.release_date IS NOT NULL
    AND g.release_date >= (now()::date - %(launch_days)s))          AS is_launch
FROM latest l
JOIN games g ON g.appid = l.appid
LEFT JOIN recent6 r ON r.appid = l.appid
LEFT JOIN base b ON b.appid = l.appid
LEFT JOIN win  w ON w.appid = l.appid
WHERE l.current_ccu >= %(min_current)s AND b.n_points >= %(min_points)s
  AND """ + not_adult("g") + """
ORDER BY 1 + (b.n_points::float/(b.n_points+%(n0)s))
              * (COALESCE(r.recent_q, l.current_ccu)::float / NULLIF(b.baseline,0) - 1) DESC NULLS LAST
LIMIT %(cand_n)s;
"""


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _descs(v, cap):
    """games.genres / games.categories（[{id,description},...] または [str]）→ 説明文の配列（先頭 cap 件）。
    「どんなゲームか」タグ用。著作物でない短い分類語のみ（export_now_ccu と同方針）。"""
    if not v:
        return []
    out = []
    for x in v:
        d = x.get("description") if isinstance(x, dict) else (x if isinstance(x, str) else None)
        if d:
            out.append(d)
        if len(out) >= cap:
            break
    return out


def base_score(shrunk_ratio, z):
    """magnitude(shrunk_ratio) を有意性でソフトに減衰。z低でも SIG_FLOOR は残す（硬い足切りなし）。"""
    if shrunk_ratio is None:
        return 0.0
    zz = 0.0 if z is None else z
    sig = SIG_FLOOR + (1 - SIG_FLOOR) * clamp(zz / Z_REF, 0, 1)
    return 1 + (shrunk_ratio - 1) * sig


def shrunk(raw, n):
    if raw is None or n is None:
        return None
    return 1 + (n / (n + N0)) * (raw - 1)


# ---------- cause（既存テーブル・防御クエリ） ----------
def cause_sets(cur, ids):
    sale, news, free, revd, jpnews = {}, {}, set(), {}, set()
    try:
        # C1: 現在の割引＋観測内の最大割引。is_best＝いまが観測範囲で最も安い（＝良いセール）か。
        cur.execute("SELECT appid, "
                    "(array_agg(discount_percent ORDER BY recorded_at DESC))[1] AS cur_disc, "
                    "max(discount_percent) AS max_disc "
                    "FROM price_snapshots WHERE appid = ANY(%s) GROUP BY appid", (ids,))
        for a, cur_d, max_d in cur.fetchall():
            cd = cur_d or 0
            sale[a] = {"pct": cd, "best": bool(cd > 0 and max_d is not None and cd >= max_d)}
    except Exception as e:
        print(f"  ⚠ cause[sale] skip: {type(e).__name__}: {e}")
    try:
        # C2: 最新告知からの経過日数も取得（news きっかけに「N日前に更新」を添える）。種別のみ・本文は載せない。
        cur.execute("SELECT appid, GREATEST(0, EXTRACT(DAY FROM (now() - max(published_at)))::int) AS days_ago "
                    "FROM announcements WHERE appid = ANY(%s) "
                    "AND published_at >= now() - make_interval(days => %s) GROUP BY appid", (ids, NEWS_DAYS))
        news = {a: (int(d) if d is not None else None) for a, d in cur.fetchall()}
    except Exception as e:
        print(f"  ⚠ cause[news] skip: {type(e).__name__}: {e}")
    try:
        cur.execute("SELECT DISTINCT appid FROM free_promos WHERE appid = ANY(%s) "
                    "AND (discount_end_date IS NULL OR discount_end_date >= now())", (ids,))
        free = {r[0] for r in cur.fetchall()}
    except Exception as e:
        print(f"  ⚠ cause[free] skip: {type(e).__name__}: {e}")
    try:
        cur.execute("WITH rn AS (SELECT DISTINCT ON (appid) appid, total_reviews FROM review_snapshots "
                    "WHERE appid = ANY(%s) ORDER BY appid, recorded_at DESC), "
                    "ro AS (SELECT DISTINCT ON (appid) appid, total_reviews FROM review_snapshots "
                    "WHERE appid = ANY(%s) AND recorded_at <= now() - make_interval(days => %s) "
                    "ORDER BY appid, recorded_at DESC) "
                    "SELECT rn.appid, rn.total_reviews - COALESCE(ro.total_reviews, rn.total_reviews) "
                    "FROM rn LEFT JOIN ro USING (appid)", (ids, ids, REV_DAYS))
        for a, dl in cur.fetchall():
            revd[a] = dl or 0
    except Exception as e:
        print(f"  ⚠ cause[review] skip: {type(e).__name__}: {e}")
    # ---- C3: 国内話題（jp_news）。jp_news は appid を持たない（設計＝名寄せ誤リンク回避）ため、
    #      ゲーム名の正規化突合で「抽象シグナル」化する（見出し/リンクは載せない・分析用）。認可：オーナー(2026-07-06)。
    #      誤マッチ源の短名は除外＝保守的。あくまで弱い補助シグナル（boost 小・上限内）。
    try:
        cur.execute("SELECT appid, name FROM games WHERE appid = ANY(%s)", (ids,))
        id_name = {a: (n or "") for a, n in cur.fetchall()}
        cur.execute("SELECT title FROM jp_news WHERE published_at >= now() - make_interval(days => %s)",
                    (JP_NEWS_DAYS,))
        ntitles = [re.sub(r"[\s\W_]+", "", (t or "")).lower() for (t,) in cur.fetchall()]
        for a, nm in id_name.items():
            nn = re.sub(r"[\s\W_]+", "", nm).lower()
            if len(nm) < JP_MIN_NAME or len(nn) < 4:
                continue  # 短名・共通語は誤マッチ源＝突合しない
            if any(nn in t for t in ntitles):
                jpnews.add(a)
    except Exception as e:
        print(f"  ⚠ cause[jp_news] skip: {type(e).__name__}: {e}")
    # ---- レビュー急増の点灯集合（方式で分岐。既定 abs は上の revd を使った現行と同一） ----
    if REV_SURGE_MODE == "relative":
        rev_surge_ids = set()
        try:
            # ro=今週開始時点の総数 / rb=さらに REV_BASELINE_DAYS 日前の総数（平常窓の起点）
            cur.execute("WITH ro AS (SELECT DISTINCT ON (appid) appid, total_reviews FROM review_snapshots "
                        "  WHERE appid = ANY(%s) AND recorded_at <= now() - make_interval(days => %s) "
                        "  ORDER BY appid, recorded_at DESC), "
                        "rb AS (SELECT DISTINCT ON (appid) appid, total_reviews FROM review_snapshots "
                        "  WHERE appid = ANY(%s) AND recorded_at <= now() - make_interval(days => %s) "
                        "  ORDER BY appid, recorded_at DESC) "
                        "SELECT ro.appid, ro.total_reviews, rb.total_reviews "
                        "FROM ro LEFT JOIN rb USING (appid)",
                        (ids, REV_DAYS, ids, REV_DAYS + REV_BASELINE_DAYS))
            weeks = REV_BASELINE_DAYS / 7.0
            for a, ro_t, rb_t in cur.fetchall():
                if rb_t is None:
                    continue  # 平常を測る履歴が無い→点灯しない（誤ラベル回避）
                d = revd.get(a, 0)
                bw = ((ro_t or 0) - rb_t) / weeks if weeks > 0 else 0.0  # 平常週ペース
                # ratio>=REL_MULT を割り算なしで判定。bw<=0（休眠→増加）は自動的に点灯。
                if d >= REV_ABS_FLOOR and d >= REV_REL_MULT * bw:
                    rev_surge_ids.add(a)
        except Exception as e:
            # 失敗時は安全側で現行(abs)へフォールバック（点灯が黙って消えるより既知挙動を保つ）
            print(f"  ⚠ cause[review:relative] skip→absフォールバック: {type(e).__name__}: {e}")
            rev_surge_ids = {a for a, dl in revd.items() if dl >= REV_SURGE}
    else:
        rev_surge_ids = {a for a, dl in revd.items() if dl >= REV_SURGE}
    return sale, news, free, revd, rev_surge_ids, jpnews


# ---------- Twitch（集計のみ・保存なし） ----------
def norm_name(name):
    s = re.sub(r"[®™©]", "", (name or "").strip())
    s = re.sub(r"\s*:\s*.*\bEdition\b.*$", "", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip()


def _tw_get(url, token):
    req = urllib.request.Request(url, headers={"Client-Id": CLIENT_ID, "Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def twitch_fetch(names):
    """ゲーム名 → (viewers, channels)。鍵が無ければ {} を返し B1 をスキップ。"""
    if not CLIENT_ID or not CLIENT_SECRET:
        print("  ⚠ Twitch鍵なし → B1（配信シグナル）をスキップし A 単独で表示。")
        return {}
    try:
        data = urllib.parse.urlencode({"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
                                       "grant_type": "client_credentials"}).encode()
        with urllib.request.urlopen("https://id.twitch.tv/oauth2/token", data=data, timeout=30) as r:
            token = json.load(r)["access_token"]
    except Exception as e:
        print(f"  ⚠ Twitch token失敗 → B1スキップ: {type(e).__name__}: {e}")
        return {}
    name_to_id = {}
    uniq = list({norm_name(n) for n in names if n})
    for i in range(0, len(uniq), 100):
        qs = "&".join("name=" + urllib.parse.quote(b) for b in uniq[i:i + 100])
        try:
            for g in _tw_get("https://api.twitch.tv/helix/games?" + qs, token).get("data", []):
                name_to_id[g["name"].lower()] = g["id"]
        except Exception as e:
            print(f"  ⚠ games lookup失敗: {type(e).__name__}: {e}")
        time.sleep(0.2)
    out = {}
    for n in names:
        gid = name_to_id.get(norm_name(n).lower())
        if not gid:
            continue
        try:
            streams = _tw_get(f"https://api.twitch.tv/helix/streams?game_id={gid}&first=100", token).get("data", [])
            out[n] = (sum(s.get("viewer_count", 0) for s in streams), len(streams))
            time.sleep(0.2)
        except Exception as e:
            print(f"  ⚠ streams失敗: {type(e).__name__}: {e}")
    return out


def b1_signal(current, tw):
    """戻り：(type_code or None, label or None, boost)。少数配信×高視聴=発掘 / 高視聴=注目。上限付き・暫定。
    type_code は公開JSON用（数値なし・②）。label は診断印字用（数値あり・公開しない）。"""
    if not tw:
        return None, None, 0.0
    v, c = tw
    if not v or v <= 0:
        return None, None, 0.0
    conc = v / max(c, 1)               # 視聴/配信（集中度）
    vpc = v / current if current else 0  # 視聴/CCU（注目度）
    if c <= B1_FEW_CH and conc >= B1_CONC_REF:
        return "b1_discovery", f"配信発掘(配信{c}・視聴{v})", B1_DISCOVERY
    if vpc >= B1_VPC_REF:
        return "b1_attention", f"配信注目(視聴/CCU {vpc:.2f})", B1_ATTENTION
    if v > 0:
        return None, f"配信あり(弱・視聴{v})", 0.0
    return None, None, 0.0


def main():
    print("=" * 88)
    print("view02 v2（A＋B1・読み取り専用・Twitch保存なし）")
    print(f"A: recent=直近{RECENT_HOURS}h p{int(RECENT_Q*100)} / 有意性ソフト(Z_REF={Z_REF},floor={SIG_FLOOR}) "
          f"/ riser=dense整合(mult={RISER_MULT}) / boost上限={BOOST_CAP}")
    print("=" * 88)
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(Q_MAIN, PARAMS)
            cols = [c[0] for c in cur.description]
            cand = [dict(zip(cols, r)) for r in cur.fetchall()]
            if not cand:
                print("0件。MIN_*を緩めるか窓を短くして再診断。")
                return
            ids = [r["appid"] for r in cand]
            sale, news, free, revd, rev_surge_ids, jpnews = cause_sets(cur, ids)
    finally:
        conn.close()

    tw = twitch_fetch([r["name"] for r in cand])  # DB接続外でTwitch取得（保存なし）

    rows = []
    for r in cand:
        sr = shrunk(r["raw_ratio"], r["n_points"])
        bs = base_score(sr, r["robust_z"])
        signals, label_parts, boost = [], [], 0.0
        sp = sale.get(r["appid"])
        if sp and sp["pct"] > 0:
            signals.append({"type": "sale", "layer": "trigger",
                            "value": {"discount_percent": int(sp["pct"]), "is_best": bool(sp["best"])}})
            label_parts.append(f"セール{sp['pct']}%" + ("(観測内最大)" if sp["best"] else "")); boost += BOOST_SALE
        if r["appid"] in news:
            da = news.get(r["appid"])
            signals.append({"type": "news", "layer": "trigger",
                            "value": (None if da is None else {"days_ago": int(da)})})
            label_parts.append(f"更新/告知({da}日前)" if da is not None else "更新/告知"); boost += BOOST_NEWS
        if r["is_launch"]:
            signals.append({"type": "launch", "layer": "trigger", "value": None})
            label_parts.append("新作"); boost += BOOST_LAUNCH
        if r["appid"] in free:
            signals.append({"type": "free_promo", "layer": "trigger", "value": None})
            label_parts.append("無料配布"); boost += BOOST_FREE
        dl = revd.get(r["appid"], 0)
        if r["appid"] in rev_surge_ids:
            signals.append({"type": "review_surge", "layer": "trigger", "value": {"delta": int(dl)}})
            label_parts.append(f"レビュー急増(+{dl})"); boost += BOOST_REVIEW
        if r["appid"] in jpnews:  # C3: 国内話題（抽象・見出しなし）
            signals.append({"type": "jp_news", "layer": "trigger", "value": None})
            label_parts.append("国内で話題"); boost += BOOST_JP
        b1type, b1label, b1boost = b1_signal(r["current_ccu"], tw.get(r["name"]))
        if b1type:  # 公開JSONは種別のみ・数値なし（②）。弱い「配信あり」はシグナルにしない。
            signals.append({"type": b1type, "layer": "trigger", "value": None}); boost += b1boost
        if b1label:
            label_parts.append(b1label)  # 印字（診断用）にはラベルを残す＝公開はしない
        boost_capped = boost >= BOOST_CAP
        boost = clamp(boost, 0, BOOST_CAP)
        eff = bs * (1 + boost)
        # 確信度（高/中/低 印字用 と high/mid/low JSON用）
        z = r["robust_z"] or 0; n = r["n_points"] or 0
        if not signals:
            conf = "低" if (z < 2 or n < 7) else "中"
            conf_code = "low" if (z < 2 or n < 7) else "mid"
            label = "原因不明"
        else:
            conf = "高" if (z >= 2 and n >= 7) else "中"
            conf_code = "high" if (z >= 2 and n >= 7) else "mid"
            if n < 5:
                conf = "低"; conf_code = "low"
            label = " + ".join(label_parts) if label_parts else "（シグナルあり）"
        r.update({"shrunk": sr, "base": bs, "eff": eff, "label": label, "conf": conf,
                  "conf_code": conf_code, "signals": signals, "boost": boost,
                  "boost_capped": boost_capped, "tw": tw.get(r["name"])})
        rows.append(r)

    rows.sort(key=lambda x: x["eff"], reverse=True)
    rows = rows[:TOP_N]

    print(f"\n総合順（eff＝有意性ソフト×上限付きブースト）上位 {len(rows)} 件：")
    print("  name                        現在  直近   平常  倍率  z  riser launch  Tw視聴/配信  推定きっかけ/確信度")
    n_unknown = 0
    for r in rows:
        if r["label"] == "原因不明":
            n_unknown += 1
        nm = (r["name"] or str(r["appid"]))[:26].ljust(26)
        base = "—" if r["baseline"] is None else f"{r['baseline']:.0f}"
        ratio = "—" if r["shrunk"] is None else f"{r['shrunk']:.2f}"
        z = "—" if r["robust_z"] is None else f"{r['robust_z']:.1f}"
        tws = "—" if not r["tw"] else f"{r['tw'][0]}/{r['tw'][1]}"
        print(f"  {nm} {str(r['current_ccu']).rjust(6)} {str(r['recent_value']).rjust(6)} "
              f"{base.rjust(6)} {ratio.rjust(5)} {z.rjust(4)} {'Y' if r['is_riser'] else '-'}    "
              f"{'Y' if r['is_launch'] else '-'}    {tws.rjust(10)}  {r['label']} / {r['conf']}")
    print(f"\n原因不明: {n_unknown}/{len(rows)}。重みは上限付き暫定（案2で学習予定）。Twitchは集計のみ・保存なし。")
    print("=" * 88)

    # ---------- 出力JSON（data/view02_rising.json・独立・上書き・可逆） ----------
    out = {
        "meta": {
            "schema_version": 1,
            "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "method": "v1_transparent_provisional",
            "experimental": True,
            "disclaimer_code": "provisional_weights_experimental",
            "window": {"base_days": BASE_DAYS, "gap_days": GAP_DAYS,
                       "recent_hours": RECENT_HOURS, "recent_quantile": RECENT_Q},
            "item_count": len(rows),
        },
        "items": [],
    }
    for i, r in enumerate(rows, 1):
        out["items"].append({
            "rank": i,
            "appid": int(r["appid"]),
            "name": r["name"],
            "detection": {
                "current_ccu": int(r["current_ccu"]),
                "recent_value": None if r["recent_value"] is None else int(round(r["recent_value"])),
                "baseline": None if r["baseline"] is None else int(round(r["baseline"])),
                "ratio": None if r["raw_ratio"] is None else round(r["raw_ratio"], 1),
                "robust_z": None if r["robust_z"] is None else round(r["robust_z"], 1),
                "n_points": int(r["n_points"]),
                "is_riser": bool(r["is_riser"]),
                "is_launch": bool(r["is_launch"]),
            },
            "genres": _descs(r.get("genres"), GENRE_MAX),        # 「どんなゲームか」タグ（games.genres・appdetails由来）
            "categories": _descs(r.get("categories"), CATEGORY_MAX),  # 補助タグ（Single-player/Co-op 等）
            "signals": r["signals"],  # 種別＋層タグ＋数値（B1は数値なし＝②）。個人は含まない。
            "prediction": {"known": bool(r["signals"]),
                           "cause_types": [s["type"] for s in r["signals"]]},
            "confidence": r["conf_code"],
            "score": {"eff": round(r["eff"], 2), "base": round(r["base"], 2),
                      "boost": round(r["boost"], 2), "boost_capped": bool(r["boost_capped"])},
        })
    try:
        out_dir = os.path.dirname(OUT_PATH)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(OUT_PATH, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"出力JSON: {OUT_PATH}（{len(rows)}件・schema v1・experimental）")
    except Exception as e:
        print(f"  ⚠ JSON書き出し失敗: {type(e).__name__}: {e}")

    print("この出力を共有 → B1の効き/きっかけ精度/暫定パラメータを調整。公開JSONは data/view02_rising.json。")


if __name__ == "__main__":
    main()
