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
env：LOOKBACK_DAYS / OUTCOME_DAYS / HIT_THRESHOLD / SMOOTH / MIN_PAIRS。
"""

import json
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

QUERY = f"""
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
  {F.feature_sql(asof='g.release_date')},
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


def learn(rows, base=None):
    """rows: [{sqlvals..., 'genres':[...], 'hit':bool}] から base率・genre命中率・WOE を算出して返す。"""
    n = len(rows)
    hits = sum(1 for r in rows if r["hit"])
    if base is None:
        base = (hits / n) if n else 0.0
    base = min(max(base, 1e-4), 0.5)
    lg_base = F.logit(base)

    # genre 命中率（スムージング）
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


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(QUERY, {"lookback": LOOKBACK_DAYS, "outcome_days": OUTCOME_DAYS})
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
        m_tr = learn(train, base=base)
        oos = evaluate(m_tr, test)
    print(f"  ホールドアウト: train={len(train)} test={len(test)}")

    # 本番モデル＝全データで学習
    model = learn(rows, base=base)

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
