"""
発売前シグナルの共有定義（学習=prelaunch_model.py と 推論=export_upcoming.py で同一の特徴量を使うための単一の源）
================================================================
なぜ共有するか：学習（過去）と推論（現在）で特徴量の定義がズレると予測が壊れる（train/serve skew）。
  だから「どのSQLでどの数字を取るか」「どう区切るか（bucket）」「どう合算するか（score）」を **1ファイルに固定**。

出せる数字（すべて自前観測・ToUクリーン・as-of＝基準時刻より前だけを見る）：
  demo_ccu        体験版の同時接続（＝体験版が実際に遊ばれている）
  twitch_peak     Twitchの最大同時視聴者（＝配信での注目）
  streamers       この作品を配信した配信者の数（＝配信の広がり）
  news_count      公式告知の本数（＝話題の活発さ・種別/本数のみ、本文は取らない）
  dev_best_peak   開発元の“他の作品”の過去最高同時接続（＝作り手の実績。DB全体を横断＝総合相関の核）
  dev_best_reviews開発元の“他の作品”の最大レビュー数（＝作り手の到達度）
  is_free         無料か（跳ねの出方が異なる）
  genre           ジャンルの過去命中率（学習時に算出したものを使う）

★思想：跳ねは「予測」する。ただし数字は**自社の過去実績で較正**した確率＝当てずっぽうではない。
  外れうることは明示し、材料が薄い作品は自動的に控えめ（基準確率）に寄る。
"""

import math

# 特徴量の順序（SQLの並びと一致させる）。genre はSQLではなくPython側で算出（学習した命中率が要るため）。
SQL_FEATURES = ["demo_ccu", "twitch_peak", "streamers", "news_count", "dev_best_peak", "dev_best_reviews", "is_free"]
FEATURE_NAMES = SQL_FEATURES + ["genre"]

# 窓（日数）。基準時刻 asof より前の直近この日数を見る。
DEMO_WIN = 14
TW_WIN = 30
NEWS_WIN = 90


def _dev_array(alias="g"):
    # jsonb 配列（開発元名の配列）→ text[]。?| で「同じ開発元の作品」を重なりで拾う。
    # developers が配列でない/NULL のときは要素抽出でエラーになるため jsonb_typeof でガード（→NULL＝該当なし）。
    return (f"(CASE WHEN jsonb_typeof({alias}.developers)='array' "
            f"THEN (SELECT array_agg(v) FROM jsonb_array_elements_text({alias}.developers) v) END)")


def feature_sql(asof, demo_win=DEMO_WIN, tw_win=TW_WIN, news_win=NEWS_WIN):
    """asof（SQLの時刻式：学習では g.release_date、推論では now()）より前だけを見る特徴量列を返す。
    返り値は SELECT のカラム列（SQL_FEATURES の順）。外側は必ず `g` エイリアスを提供すること。"""
    da = _dev_array("g")
    return f"""
      (SELECT max(pc.player_count) FROM player_counts pc
         JOIN games dg ON dg.appid = pc.appid AND dg.fullgame_appid = g.appid
        WHERE pc.recorded_at < {asof} AND pc.recorded_at >= {asof} - make_interval(days => {demo_win})) AS demo_ccu,
      (SELECT max(ts.viewers) FROM twitch_snapshots ts
        WHERE ts.appid = g.appid AND ts.recorded_at < {asof} AND ts.recorded_at >= {asof} - make_interval(days => {tw_win})) AS twitch_peak,
      (SELECT count(DISTINCT sa.twitch_user_id) FROM streamer_activity sa
        WHERE sa.appid = g.appid AND sa.recorded_at < {asof} AND sa.recorded_at >= {asof} - make_interval(days => {tw_win})) AS streamers,
      (SELECT count(*) FROM announcements a
        WHERE a.appid = g.appid AND a.published_at < {asof} AND a.published_at >= {asof} - make_interval(days => {news_win})) AS news_count,
      (SELECT max(pc.player_count) FROM player_counts pc
         JOIN games a2 ON a2.appid = pc.appid
        WHERE a2.appid <> g.appid AND a2.developers ?| {da} AND pc.recorded_at < {asof}) AS dev_best_peak,
      (SELECT max(rs.total_reviews) FROM review_snapshots rs
         JOIN games a3 ON a3.appid = rs.appid
        WHERE a3.appid <> g.appid AND a3.developers ?| {da} AND rs.recorded_at < {asof}) AS dev_best_reviews,
      g.is_free AS is_free
    """


def bucketize(name, v, genre_rates=None, base=None):
    """特徴量の値を数個の区切り（bucket）に落とす。学習・推論で同じ関数を使う＝定義を固定。"""
    if name == "demo_ccu":
        if v is None or v <= 0: return "none"
        if v < 50: return "low"
        if v < 500: return "mid"
        return "high"
    if name == "twitch_peak":
        if v is None or v <= 0: return "none"
        if v < 100: return "low"
        if v < 1000: return "mid"
        return "high"
    if name == "streamers":
        if v is None or v <= 0: return "none"
        if v < 5: return "low"
        return "high"
    if name == "news_count":
        if v is None or v <= 0: return "none"
        if v < 3: return "low"
        return "high"
    if name == "dev_best_peak":
        if v is None or v <= 0: return "none"
        if v < 1000: return "low"
        if v < 10000: return "mid"
        return "high"
    if name == "dev_best_reviews":
        if v is None or v <= 0: return "none"
        if v < 1000: return "low"
        return "high"
    if name == "is_free":
        return "free" if v else "paid"
    if name == "genre":
        # v は genres(リスト)。学習した per-genre 命中率の最大を base と比べて区切る。
        if not v or not genre_rates or not base or base <= 0:
            return "na"
        best = 0.0
        for gname in v:
            r = genre_rates.get(gname)
            if r and r.get("rate") is not None:
                best = max(best, r["rate"])
        if best <= 0: return "na"
        lift = best / base
        if lift < 0.8: return "cold"
        if lift < 1.5: return "warm"
        return "hot"
    return "na"


def logit(p):
    p = min(max(p, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))


def sigmoid(x):
    if x >= 0:
        z = math.exp(-x); return 1 / (1 + z)
    z = math.exp(x); return z / (1 + z)


def score(model, sql_values, genres):
    """model（prelaunch_model.json）と 特徴量の生値 dict を受け、跳ね確率と寄与内訳を返す。
    sql_values: {name: value}（SQL_FEATURES 分）。genres: リスト。
    返り値: {"prob": float, "base": float, "factors": [{name, bucket, woe, dir}], "active": int}"""
    base = model.get("base_rate") or 0.0
    woe = model.get("woe") or {}
    genre_rates = model.get("genre_rates") or {}
    lg = logit(base)
    factors = []
    active = 0
    for name in FEATURE_NAMES:
        if name == "genre":
            b = bucketize("genre", genres, genre_rates=genre_rates, base=base)
        else:
            b = bucketize(name, sql_values.get(name))
        w = ((woe.get(name) or {}).get(b))
        if w is None:
            w = 0.0
        lg += w
        # 「材料あり」＝none/na/paid 以外
        is_active = b not in ("none", "na", "paid")
        if is_active:
            active += 1
        if abs(w) >= 0.01:
            factors.append({"name": name, "bucket": b, "woe": round(w, 4),
                            "dir": "up" if w > 0 else "down"})
    prob = sigmoid(lg)
    factors.sort(key=lambda f: abs(f["woe"]), reverse=True)
    return {"prob": prob, "base": base, "factors": factors, "active": active}
