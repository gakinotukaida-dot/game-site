"""
羽根予想モデルの学習（読み取り専用）── 2026-07-07 / v1
================================================================
役割：発売前シグナル → 発売後の跳ね を、自社の過去実績から **較正した確率モデル（スコアカード）** として学習し、
      data/prelaunch_model.json に書き出す。推論（export_upcoming.py）はこの JSON を読んで各作品の跳ね確率を出す。

思想：跳ねは「予測」する。ただし各シグナルの重みは **実際の命中率（base rate）から算出**＝当てずっぽうではない。
  - as-of（リーク無し）：特徴量は発売日より前だけ、結果は発売日以降だけ。
  - スコアカード＝Naive-Bayes 的な log-odds 合算（解釈可能・材料が薄い作品は自動で基準確率に寄る）。
  - honest な強さ指標：70/30 ホールドアウトで **out-of-sample** の上位十分位リフトを測って載せる（in-sample の過大評価を避ける）。
  - 本番モデルは全データで再学習（データを無駄にしない）。

線：DBは読み取り専用（SELECTのみ）。書き込みは data/prelaunch_model.json 1ファイルのみ・毎回上書き＝可逆。
    著作物は載せない（重み・命中率・件数のみ）。
env：LOOKBACK_DAYS / OUTCOME_DAYS / HIT_THRESHOLD / SMOOTH / MIN_PAIRS / MODEL_KIND / SHADOW_COMPARE。

★MODEL_KIND（2026-10-02 追加・既定 nb＝従来と完全同一）：
  nb＝上記の Naive-Bayes 的スコアカード。各特徴量の重みを「1つずつ別々に」命中率から出して足す。
      弱点＝同じ情報を測る特徴量（開発元の最高同接と最大レビュー数、web の3つ等）を二重三重に足すため、
      上位帯ほど確率が高く出すぎる（土台 0808-0915-05／0916-2242-G1 の自信過剰）。
  lr＝L2 正則化つきロジスティック回帰。特徴量・区切り（bucketize）は nb と1つも変えずに、全特徴量を
      「同時に」当てはめて重みを決める＝重なった特徴量の効き目は自動で分け合う。正則化の強さは
      train の中だけの 5-fold 交差検証で選ぶ（test を見ない）。外部ライブラリ不要（同じ区切りの行を
      まとめて数え、ニュートン法で解く）。出力の形は nb と同じ（woe 欄に係数・kind="lr"・intercept を追加）。
  戻し方＝MODEL_KIND を消すか nb にするだけ。
SHADOW_COMPARE=1（影モード用）：同じ 70/30 分割で nb と lr を両方学習し、test の較正帯・指標と
  事前登録の合格条件（本人確定 2026-10-02）の判定を payload の shadow_compare に載せる。
"""

import json
import math
import os
from datetime import datetime

import psycopg2

import prelaunch_features as F
from _filters import not_adult

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/prelaunch_model.json"

LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS") or "180")
OUTCOME_DAYS = int(os.environ.get("OUTCOME_DAYS") or "14")
HIT_THRESHOLD = int(os.environ.get("HIT_THRESHOLD") or "1000")
SMOOTH = float(os.environ.get("SMOOTH") or "8")      # 加法スムージングの擬似件数（小さいbucketを基準へ収縮）
MIN_PAIRS = int(os.environ.get("MIN_PAIRS") or "300")  # これ未満なら readiness=collecting（確率は控えめ運用推奨）
MODEL_KIND = (os.environ.get("MODEL_KIND") or "nb").strip().lower()   # nb（既定・従来）／lr
if MODEL_KIND not in ("nb", "lr"):
    MODEL_KIND = "nb"   # 想定外の値は保守側（従来）へ倒す
SHADOW_COMPARE = (os.environ.get("SHADOW_COMPARE") or "").strip().lower() in ("1", "true", "yes")
# lr の正則化の候補（交差検証で1つ選ぶ）。大きいほど重みを0へ縮める＝材料の薄い区切りを控えめにする。
LR_L2_GRID = [float(x) for x in (os.environ.get("LR_L2_GRID") or "0.3,1,3,10,30,100").split(",") if x.strip()]
LR_FOLDS = int(os.environ.get("LR_FOLDS") or "5")
# 較正帯（export_scorecard.CALIB_EDGES と同じ区切り＝サイトの較正表と同じ物差しで比べる）。
CALIB_EDGES = [0.0, 0.02, 0.05, 0.10, 0.20, 0.40, 1.01]
TOP_BAND_LO = 0.40          # 「上位帯」＝予測40%以上（0916-2242-G1 の上位帯と同じ）
TOP_BAND_MIN_N = 20         # 上位帯の件数がこれ未満なら①は上位10%で判定（shadow_compare の c1_basis）

def build_query(web_ok):
    return f"""
WITH {F.cte_prelude()},
released AS (
  SELECT g.appid, g.name, g.release_date, g.developers, g.is_free, g.genres
  FROM games g
  WHERE g.release_date IS NOT NULL
    AND g.release_date <= now()::date
    AND g.release_date >= (now() - make_interval(days => %(lookback)s))::date
    AND g.coming_soon IS NOT TRUE
    AND {not_adult('g')}
),
{F.dev_best_cte('released', 's.release_date')}
SELECT g.appid, g.genres,
  {F.feature_sql(asof='g.release_date', web_ok=web_ok)},
  db.dev_best_peak, db.dev_best_reviews,
  (SELECT max(pc.player_count) FROM player_counts pc
     WHERE pc.appid = g.appid
       AND pc.recorded_at >= g.release_date
       AND pc.recorded_at < g.release_date + make_interval(days => %(outcome_days)s)) AS launch_peak
FROM released g
LEFT JOIN dev_best db ON db.appid = g.appid
"""


def _holdout_is_train(appid):
    """70/30 の決定的ホールドアウト割り当て（RNG不使用）。appidの末尾偏りを乗算ハッシュ(Knuth)で無相関化してから割る。"""
    h = (int(appid) * 2654435761) & 0xFFFFFFFF
    return (h % 100) < 70


def _genres_list(genres):
    out = []
    if isinstance(genres, list):
        for x in genres:
            if isinstance(x, dict):
                d = x.get("description")
                if d and str(d).strip():
                    out.append(str(d).strip())
    return out


def _clip_base(rows, base=None):
    n = len(rows)
    hits = sum(1 for r in rows if r["hit"])
    if base is None:
        base = (hits / n) if n else 0.0
    return n, hits, min(max(base, 1e-4), 0.5)


def _genre_rates(rows, base):
    """genre 命中率（スムージング）。nb・lr で共通（genre の区切りはこの命中率で決まる）。"""
    g_tot, g_hit = {}, {}
    for r in rows:
        for gname in r["genres"]:
            g_tot[gname] = g_tot.get(gname, 0) + 1
            if r["hit"]:
                g_hit[gname] = g_hit.get(gname, 0) + 1
    genre_rates = {}
    for gname, tot in g_tot.items():
        h = g_hit.get(gname, 0)
        rate = (h + SMOOTH * base) / (tot + SMOOTH)
        genre_rates[gname] = {"n": tot, "hits": h, "rate": round(rate, 5)}
    return genre_rates


def learn(rows, base=None):
    """rows: [{sqlvals..., 'genres':[...], 'hit':bool}] から base率・genre命中率・WOE を算出して返す。"""
    n, hits, base = _clip_base(rows, base)
    lg_base = F.logit(base)

    # genre 命中率（スムージング）
    genre_rates = _genre_rates(rows, base)

    # 各特徴量 × bucket の命中率 → WOE（= logit(p_bucket) - logit(base)）
    counts = {name: {} for name in F.FEATURE_NAMES}   # name -> bucket -> [n, hits]
    for r in rows:
        for name in F.FEATURE_NAMES:
            if name == "genre":
                b = F.bucketize("genre", r["genres"], genre_rates=genre_rates, base=base)
            else:
                b = F.bucketize(name, r.get(name))
            slot = counts[name].setdefault(b, [0, 0])
            slot[0] += 1
            if r["hit"]:
                slot[1] += 1
    woe = {}
    for name, buckets in counts.items():
        woe[name] = {}
        for b, (nb, hb) in buckets.items():
            pb = (hb + SMOOTH * base) / (nb + SMOOTH)
            woe[name][b] = round(F.logit(pb) - lg_base, 5)

    return {"base_rate": round(base, 6), "genre_rates": genre_rates, "woe": woe,
            "n": n, "hits": hits}


def evaluate(model, rows):
    """model で rows を採点し、out-of-sample の識別力（上位十分位リフト・平均確率差）を返す。"""
    if not rows:
        return {}
    scored = []
    for r in rows:
        s = F.score(model, {k: r.get(k) for k in F.SQL_FEATURES}, r["genres"])
        scored.append((s["prob"], r["hit"]))
    scored.sort(key=lambda x: x[0], reverse=True)
    n = len(scored)
    base = sum(1 for _, h in scored if h) / n if n else 0.0
    k = max(1, n // 10)
    top = scored[:k]
    top_rate = sum(1 for _, h in top if h) / len(top)
    mean_hit = sum(p for p, h in scored if h) / max(1, sum(1 for _, h in scored if h))
    mean_miss = sum(p for p, h in scored if not h) / max(1, sum(1 for _, h in scored if not h))
    return {
        "eval_n": n,
        "eval_base": round(base, 5),
        "top_decile_rate": round(top_rate, 5),
        "top_decile_lift": round(top_rate / base, 2) if base else None,
        "mean_prob_hit": round(mean_hit, 5),
        "mean_prob_miss": round(mean_miss, 5),
    }


# ---------------------------------------------------------------------------
# MODEL_KIND=lr：L2 正則化つきロジスティック回帰（2026-10-02 追加）。
# 特徴量と区切りは nb と同一（F.bucketize）。各特徴量の区切りを 0/1 の列にし、全列を同時に当てはめる。
# 同じ区切りの組み合わせの行は (件数, 跳ね数) にまとめて数える＝行数ではなく組み合わせ数の計算量で済む。
# ---------------------------------------------------------------------------

def _bucket_key(r, genre_rates, base):
    return tuple(
        (name, F.bucketize("genre", r["genres"], genre_rates=genre_rates, base=base)
         if name == "genre" else F.bucketize(name, r.get(name)))
        for name in F.FEATURE_NAMES)


def _collapse(rows, genre_rates, base):
    pats = {}
    for r in rows:
        slot = pats.setdefault(_bucket_key(r, genre_rates, base), [0, 0])
        slot[0] += 1
        if r["hit"]:
            slot[1] += 1
    return pats


def _solve(A, b):
    """連立一次方程式 A x = b（部分ピボットつきガウス消去）。A は正則化で正定値。"""
    n = len(b)
    M_ = [list(A[i]) + [b[i]] for i in range(n)]
    for c in range(n):
        p = max(range(c, n), key=lambda i: abs(M_[i][c]))
        M_[c], M_[p] = M_[p], M_[c]
        piv = M_[c][c]
        if abs(piv) < 1e-12:
            raise ValueError("singular system")
        for i in range(c + 1, n):
            f = M_[i][c] / piv
            if f:
                Mi, Mc = M_[i], M_[c]
                for j in range(c, n + 1):
                    Mi[j] -= f * Mc[j]
    x = [0.0] * n
    for i in range(n - 1, -1, -1):
        x[i] = (M_[i][n] - sum(M_[i][j] * x[j] for j in range(i + 1, n))) / M_[i][i]
    return x


def _fit_lr(rows, genre_rates, base, l2):
    """rows を当てはめて、score() がそのまま読める形のモデル dict を返す（kind="lr"）。"""
    pats = _collapse(rows, genre_rates, base)
    col_keys = sorted({fb for key in pats for fb in key})
    cols = {fb: i + 1 for i, fb in enumerate(col_keys)}   # 0 は切片
    k = len(cols) + 1
    data = [([0] + [cols[fb] for fb in key], n, h) for key, (n, h) in pats.items()]
    beta = [0.0] * k
    beta[0] = F.logit(base)
    iters = 0
    for iters in range(1, 101):
        g = [0.0] * k
        H = [[0.0] * k for _ in range(k)]
        for idx, n, h in data:
            p = F.sigmoid(sum(beta[j] for j in idx))
            resid = h - n * p
            w = n * p * (1 - p)
            for a in idx:
                g[a] += resid
                Ha = H[a]
                for b_ in idx:
                    Ha[b_] += w
        for j in range(1, k):          # 切片は縮めない
            g[j] -= l2 * beta[j]
            H[j][j] += l2
        H[0][0] += 1e-9
        step = _solve(H, g)
        mx = max(abs(s) for s in step)
        if mx > 5:                      # 1回の歩幅を抑える（発散よけ）
            step = [s * 5 / mx for s in step]
        beta = [beta[j] + step[j] for j in range(k)]
        if mx < 1e-7:
            break
    woe = {name: {} for name in F.FEATURE_NAMES}
    for (name, b), i in cols.items():
        woe[name][b] = round(beta[i], 5)
    return {"kind": "lr", "base_rate": round(base, 6), "genre_rates": genre_rates, "woe": woe,
            "intercept": round(beta[0], 6), "l2": l2, "iters": iters}


def _fold_of(appid, k):
    """交差検証の割り当て（RNG不使用・holdout とは別の乗数で無相関化）。train の中だけで使う。"""
    h = (int(appid) * 2246822519) & 0xFFFFFFFF
    return (h >> 7) % k


def _logloss(model, rows):
    s = 0.0
    for r in rows:
        p = F.score(model, {k: r.get(k) for k in F.SQL_FEATURES}, r["genres"])["prob"]
        p = min(max(p, 1e-9), 1 - 1e-9)
        s -= math.log(p) if r["hit"] else math.log(1 - p)
    return s


def learn_lr(rows, base=None):
    """ロジスティック回帰で学習。正則化の強さは rows の中だけの k-fold 交差検証（対数損失が最小）で選ぶ。"""
    n, hits, base = _clip_base(rows, base)
    base = round(base, 6)   # 推論側は JSON の base_rate（6桁）で genre を区切る＝学習も同じ値で区切る
    cv = []
    grid = LR_L2_GRID or [3.0]
    if len(grid) > 1 and LR_FOLDS >= 2 and n >= 200:
        for l2 in grid:
            loss, cnt = 0.0, 0
            for f in range(LR_FOLDS):
                tr = [r for r in rows if _fold_of(r["appid"], LR_FOLDS) != f]
                va = [r for r in rows if _fold_of(r["appid"], LR_FOLDS) == f]
                if not tr or not va:
                    continue
                _, _, b_tr = _clip_base(tr)
                b_tr = round(b_tr, 6)
                m = _fit_lr(tr, _genre_rates(tr, b_tr), b_tr, l2)
                loss += _logloss(m, va)
                cnt += len(va)
            cv.append({"l2": l2, "logloss": round(loss / cnt, 6) if cnt else None})
        ok = [c for c in cv if c["logloss"] is not None]
        l2_best = min(ok, key=lambda c: c["logloss"])["l2"] if ok else grid[len(grid) // 2]
    else:
        l2_best = grid[len(grid) // 2]
    model = _fit_lr(rows, _genre_rates(rows, base), base, l2_best)
    model.update({"cv": cv, "n": n, "hits": hits})
    return model


def learn_model(rows, base=None, kind=None):
    """MODEL_KIND に応じて学習（既定 nb＝従来の learn と完全同一）。export_scorecard の holdout もこれを使う。"""
    kind = kind or MODEL_KIND
    return learn_lr(rows, base=base) if kind == "lr" else learn(rows, base=base)


# ---------------------------------------------------------------------------
# 影比較（SHADOW_COMPARE=1）：同じ 70/30 分割で nb と lr を並べる。物差しはサイトの較正表と同じ区切り。
# 合格条件（本人確定 2026-10-02・事前登録＝結果を見てから動かさない）：
#   ① 上位帯（予測40%以上）の |予測平均−実測| が、lr は nb の半分以下（両者とも上位帯 n≥20 のとき。
#      足りなければ各モデルの上位10%で同じ基準＝c1_basis に記録）
#   ② 上位10%の倍率（lift）が、lr は nb の 0.9 倍以上
#   ③ Brier（予測の外れ具合）が、lr は nb 以下
# ---------------------------------------------------------------------------

def _calibration(scored):
    bins = []
    for i in range(len(CALIB_EDGES) - 1):
        lo, hi = CALIB_EDGES[i], CALIB_EDGES[i + 1]
        grp = [(p, h) for p, h in scored if lo <= p < hi]
        n = len(grp)
        bins.append({"range": [round(lo, 4), round(hi if hi <= 1 else 1.0, 4)], "n": n,
                     "pred_mean": round(sum(p for p, _ in grp) / n, 4) if n else None,
                     "actual_rate": round(sum(1 for _, h in grp if h) / n, 4) if n else None})
    return bins


def _cmp_metrics(scored):
    n = len(scored)
    hits = sum(1 for _, h in scored if h)
    base = hits / n if n else 0.0
    s = sorted(scored, key=lambda x: x[0], reverse=True)
    k = max(1, n // 10)
    top = s[:k]
    top_rate = sum(1 for _, h in top if h) / len(top) if top else 0.0
    top_pred = sum(p for p, _ in top) / len(top) if top else 0.0
    band = [(p, h) for p, h in scored if p >= TOP_BAND_LO]
    bn = len(band)
    bp = sum(p for p, _ in band) / bn if bn else None
    ba = sum(1 for _, h in band if h) / bn if bn else None
    return {
        "n": n, "hits": hits, "base_rate": round(base, 5),
        "top_decile_lift": round(top_rate / base, 2) if base else None,
        "brier": round(sum((p - (1 if h else 0)) ** 2 for p, h in scored) / n, 6) if n else None,
        "top_band": {"n": bn, "pred_mean": round(bp, 4) if bp is not None else None,
                     "actual_rate": round(ba, 4) if ba is not None else None,
                     "abs_gap": round(abs(bp - ba), 4) if bn else None},
        # 参考（条件外）：上位10%の予測平均と実測。上位帯の件数が少ないときの補助。
        "top_decile_ref": {"n": len(top), "pred_mean": round(top_pred, 4), "actual_rate": round(top_rate, 4)},
    }


def shadow_compare(train, test, base):
    out = {"split": {"train_n": len(train), "test_n": len(test)}, "models": {}}
    for kind in ("nb", "lr"):
        m = learn_model(train, base=base, kind=kind)
        scored = [(F.score(m, {k: r.get(k) for k in F.SQL_FEATURES}, r["genres"])["prob"], r["hit"])
                  for r in test]
        entry = {"metrics": _cmp_metrics(scored), "calibration": _calibration(scored)}
        if kind == "lr":
            entry["l2"] = m.get("l2")
            entry["cv"] = m.get("cv")
        out["models"][kind] = entry
    nb, lr = out["models"]["nb"]["metrics"], out["models"]["lr"]["metrics"]
    c1 = None
    c1_basis = None
    if nb["top_band"]["n"] >= TOP_BAND_MIN_N and lr["top_band"]["n"] >= TOP_BAND_MIN_N:
        c1 = lr["top_band"]["abs_gap"] <= nb["top_band"]["abs_gap"] / 2
        c1_basis = "top_band_40pct"
    else:
        # 上位帯の件数が足りないとき（較正が直ると40%以上を出す件数が減りうる）は、
        # 各モデルの上位10%（件数は必ず test の1割）の「予測平均−実測」で同じ基準（半分以下）を当てる。
        # ★この代替は結果を見る前（合成データでの動作確認時・2026-10-02）に Claude が決めた。本人の拒否権あり。
        gn = abs(nb["top_decile_ref"]["pred_mean"] - nb["top_decile_ref"]["actual_rate"])
        gl = abs(lr["top_decile_ref"]["pred_mean"] - lr["top_decile_ref"]["actual_rate"])
        c1 = gl <= gn / 2
        c1_basis = "top_decile_fallback"
    c2 = (lr["top_decile_lift"] >= 0.9 * nb["top_decile_lift"]
          if (lr["top_decile_lift"] is not None and nb["top_decile_lift"] is not None) else None)
    c3 = (lr["brier"] <= nb["brier"]) if (lr["brier"] is not None and nb["brier"] is not None) else None
    conds = [c1, c2, c3]
    verdict = "判定不能" if any(c is None for c in conds) else ("合格" if all(conds) else "不合格")
    out["criteria"] = {
        "c1_top_band_gap_halved": c1, "c1_basis": c1_basis, "c2_lift_ge_0.9x": c2, "c3_brier_not_worse": c3,
        "verdict": verdict,
        "note": ("事前登録（本人確定 2026-10-02）。上位帯＝予測40%以上。どちらかの上位帯が n<20 のときは各モデルの上位10%で①を判定（c1_basis）。"
                 "開発元実績の近似（学習時に発売後の記録を含む）は nb・lr の両方に同じく効くため、比較の公平は保たれるが絶対値は楽観寄り。"),
    }
    return out


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            web_ok = F.web_mentions_exists(cur)   # web_mentions が無ければ web_* は NULL（無影響）
            cur.execute(build_query(web_ok), {"lookback": LOOKBACK_DAYS, "outcome_days": OUTCOME_DAYS})
            cols = [d[0] for d in cur.description]
            recs = cur.fetchall()
    finally:
        conn.close()

    # 行を dict 化。結果（launch_peak）が無い＝発売後未観測＝対にならない。
    rows = []
    for rec in recs:
        d = dict(zip(cols, rec))
        lp = d.get("launch_peak")
        if lp is None:
            continue
        row = {name: d.get(name) for name in F.SQL_FEATURES}
        row["appid"] = d.get("appid")
        row["genres"] = _genres_list(d.get("genres"))
        row["hit"] = int(lp) >= HIT_THRESHOLD
        rows.append(row)

    n_pairs = len(rows)
    base = (sum(1 for r in rows if r["hit"]) / n_pairs) if n_pairs else 0.0

    # 70/30 ホールドアウト（appid で決定的に分割＝再現性・RNG不使用）で OOS の強さを測る。
    # ※ Steam の appid は末尾0が多く %10 が激しく偏る（→testが空になる）ので、乗算ハッシュで無相関化してから割る。
    train = [r for r in rows if _holdout_is_train(r["appid"])]
    test = [r for r in rows if not _holdout_is_train(r["appid"])]
    oos = {}
    if len(train) >= 50 and len(test) >= 20:
        m_tr = learn_model(train, base=base)
        oos = evaluate(m_tr, test)
    print(f"  ホールドアウト: train={len(train)} test={len(test)}・方式 {MODEL_KIND}")

    # 本番モデル＝全データで学習
    model = learn_model(rows, base=base)

    ready = (n_pairs >= MIN_PAIRS) and bool(oos.get("top_decile_lift") and oos["top_decile_lift"] > 1.3)
    readiness = "validated" if ready else "collecting"

    # 各特徴量の“効き”を一覧（bucket ごとの命中率）＝人間が中身を確認できるように
    feature_report = {}
    for name in F.FEATURE_NAMES:
        rep = {}
        for r in rows:
            b = (F.bucketize("genre", r["genres"], genre_rates=model["genre_rates"], base=model["base_rate"])
                 if name == "genre" else F.bucketize(name, r.get(name)))
            slot = rep.setdefault(b, [0, 0])
            slot[0] += 1
            if r["hit"]:
                slot[1] += 1
        feature_report[name] = {b: {"n": nb, "hits": hb, "rate": round(hb / nb, 4) if nb else None,
                                    "woe": model["woe"][name].get(b)}
                                for b, (nb, hb) in sorted(rep.items())}

    payload = {
        "view": "prelaunch_model",
        "schema": "prelaunch_model_v1",
        "note": ("羽根予想モデル（自社実績で較正したスコアカード）。跳ね確率は log-odds 合算＝解釈可能・材料が薄いほど基準確率に寄る。"
                 "強さ指標 top_decile_lift は 70/30 ホールドアウトの out-of-sample。readiness=collecting の間は控えめ運用。"),
        "generated_at": datetime.now().astimezone().isoformat(),
        "params": {"lookback_days": LOOKBACK_DAYS, "outcome_days": OUTCOME_DAYS,
                   "hit_threshold": HIT_THRESHOLD, "smooth": SMOOTH, "min_pairs": MIN_PAIRS},
        "readiness": readiness,
        "n_pairs": n_pairs,
        "hits": model["hits"],
        "base_rate": model["base_rate"],
        "features": F.FEATURE_NAMES,
        "woe": model["woe"],
        "genre_rates": model["genre_rates"],
        "validation_oos": oos,
        "feature_report": feature_report,
    }
    # lr のときだけ足す（nb の JSON は従来と1バイトも変えない）。
    if model.get("kind") == "lr":
        payload["kind"] = "lr"
        payload["intercept"] = model["intercept"]
        payload["l2"] = model["l2"]
        payload["cv"] = model.get("cv")
        payload["note"] = ("羽根予想モデル（ロジスティック回帰・L2正則化・自社実績で学習）。全特徴量を同時に当てはめ、"
                           "重なった特徴量の効き目を分け合う。woe 欄は回帰係数（切片は intercept）。"
                           "強さ指標 top_decile_lift は 70/30 ホールドアウトの out-of-sample。readiness=collecting の間は控えめ運用。")
    if SHADOW_COMPARE and len(train) >= 50 and len(test) >= 20:
        cmp_ = shadow_compare(train, test, base)
        payload["shadow_compare"] = cmp_
        for kind, e in cmp_["models"].items():
            mt = e["metrics"]
            print(f"  [影比較] {kind}: 上位帯 予測{mt['top_band']['pred_mean']}・実測{mt['top_band']['actual_rate']}"
                  f"・n={mt['top_band']['n']}／lift×{mt['top_decile_lift']}／Brier {mt['brier']}")
        print(f"  [影比較] 判定＝{cmp_['criteria']['verdict']} {cmp_['criteria']}")

    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")

    print(f"書き出し: {OUT_PATH}")
    print(f"  学習対（発売後CCUあり）{n_pairs} 件・跳ね(hit≥{HIT_THRESHOLD}) {model['hits']} 件・base={model['base_rate']:.4f}")
    print(f"  OOS(70/30ホールドアウト): {oos}")
    print(f"  readiness = {readiness}")
    print("  --- 特徴量の効き（bucket: 命中率 / woe）---")
    for name in F.FEATURE_NAMES:
        parts = []
        for b, s in feature_report[name].items():
            parts.append(f"{b}:{s['rate']}({s['woe']:+.2f},n={s['n']})")
        print(f"   {name:16} " + "  ".join(parts))


if __name__ == "__main__":
    main()
