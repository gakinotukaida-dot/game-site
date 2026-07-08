#!/usr/bin/env python3
"""
羽根予想の“的中率スコアカード”（読み取り専用・公開の答え合わせ）── 2026-07-08 / v1
================================================================
役割（信頼構築の核）：羽根予想が実際どれだけ当たっているかを、**あと出し無し（as-of）** で公開する。
  出すもの（すべて集計値・検証可能）：
   - 較正（calibration）：「◯%」と予測した群が、実際に何%跳ねたか（予測と実測が一致するほど信頼できる）。
   - 上位十分位リフト・Brier・高確度群の的中率：モデルの識別力と当て具合。
   - 直近の答え合わせ例：発売済み作品の「予測% → 実際に跳ねたか/最大同時接続」（人が目で確かめられる）。

2つの証拠を併記する（正直に強さの違いを明示）：
  A) holdout（バックテスト）：過去の発売済みを 70/30 に決定的分割し、**train だけで学習したモデル**で test を採点。
     → in-sample の過大評価を避けた honest な成績。今すぐ十分な件数が出る（土台）。
     ※ 学習(prelaunch_model)と同じ分割関数・同じ learn/score を再利用＝方法論が完全一致。
  B) live（実運用）：prediction_log に貯まった「発売前に記録した予測 → 発売後の結果」の対。
     → 予測時点で結果も学習データも未知＝最も硬い真の out-of-sample。件数は日々増える。

線（絶対に破らない）：
- DB は読み取り専用（SELECTのみ）。書き込みは data/prediction_scorecard.json 1ファイルだけ・毎回上書き＝可逆。
- リーク無し（as-of）：シグナルは発売日より前、結果は発売日以降だけ。未来は使わない。
- 成人向けは対象外。著作物は載せない（名前・appid・数値のみ。名前は既存 view と同様に公開情報）。
env：LOOKBACK_DAYS / OUTCOME_DAYS / HIT_THRESHOLD / EXAMPLES。
"""
import json
import os
from datetime import datetime

import psycopg2

import prelaunch_features as F
import prelaunch_model as M          # _holdout_is_train / learn を共有＝方法論を学習と一致
from _filters import not_adult

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/prediction_scorecard.json"

LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS") or "180")
OUTCOME_DAYS = int(os.environ.get("OUTCOME_DAYS") or "14")
HIT_THRESHOLD = int(os.environ.get("HIT_THRESHOLD") or "1000")
EXAMPLES = int(os.environ.get("EXAMPLES") or "15")

# 較正ビン（予測確率の絶対区切り。base(≈0.036) 前後を細かく見る）。
CALIB_EDGES = [0.0, 0.02, 0.05, 0.10, 0.20, 0.40, 1.01]

# holdout 用：発売済み＋as-of 特徴量＋発売後ピーク（prelaunch_model.QUERY と同じ定義。name/release_date を追加）。
QUERY = f"""
WITH {F.cte_prelude()},
released AS (
  SELECT g.appid, g.name, g.release_date, g.is_free, g.genres
  FROM games g
  WHERE g.release_date IS NOT NULL
    AND g.release_date <= now()::date
    AND g.release_date >= (now() - make_interval(days => %(lookback)s))::date
    AND g.coming_soon IS NOT TRUE
    AND {not_adult('g')}
),
{F.dev_best_cte('released', 's.release_date')}
SELECT g.appid, g.name, g.release_date, g.genres,
  {F.feature_sql(asof='g.release_date')},
  db.dev_best_peak, db.dev_best_reviews,
  (SELECT max(pc.player_count) FROM player_counts pc
     WHERE pc.appid = g.appid
       AND pc.recorded_at >= g.release_date
       AND pc.recorded_at < g.release_date + make_interval(days => %(outcome_days)s)) AS launch_peak
FROM released g
LEFT JOIN dev_best db ON db.appid = g.appid
"""


def _genres_list(genres):
    out = []
    if isinstance(genres, list):
        for x in genres:
            if isinstance(x, dict):
                d = x.get("description")
                if d and str(d).strip():
                    out.append(str(d).strip())
    return out


def _rel_iso(rd):
    try:
        return rd.isoformat()
    except AttributeError:
        return str(rd) if rd is not None else None


def _calibration(scored):
    """scored: [(prob, hit)] → 予測確率ビンごとの [n, 予測平均, 実測跳ね率]。"""
    bins = []
    for i in range(len(CALIB_EDGES) - 1):
        lo, hi = CALIB_EDGES[i], CALIB_EDGES[i + 1]
        grp = [(p, h) for p, h in scored if (p >= lo and p < hi)]
        n = len(grp)
        if n == 0:
            bins.append({"range": [round(lo, 4), round(hi, 4)], "n": 0,
                         "pred_mean": None, "actual_rate": None})
            continue
        pred_mean = sum(p for p, _ in grp) / n
        actual = sum(1 for _, h in grp if h) / n
        bins.append({"range": [round(lo, 4), round(hi if hi <= 1 else 1.0, 4)], "n": n,
                     "pred_mean": round(pred_mean, 4), "actual_rate": round(actual, 4)})
    return bins


def _metrics(scored):
    """scored: [(prob, hit)] → 全体の識別力・当て具合。"""
    n = len(scored)
    if n == 0:
        return {}
    hits = sum(1 for _, h in scored if h)
    base = hits / n
    s = sorted(scored, key=lambda x: x[0], reverse=True)
    k = max(1, n // 10)
    top = s[:k]
    top_rate = sum(1 for _, h in top if h) / len(top)
    brier = sum((p - (1 if h else 0)) ** 2 for p, h in scored) / n
    hc = [(p, h) for p, h in scored if base > 0 and p >= 3 * base]   # 高確度群（base の3倍以上）
    hc_rate = (sum(1 for _, h in hc if h) / len(hc)) if hc else None
    return {
        "n": n, "hits": hits, "base_rate": round(base, 5),
        "top_decile_n": len(top), "top_decile_rate": round(top_rate, 5),
        "top_decile_lift": round(top_rate / base, 2) if base else None,
        "brier": round(brier, 5),
        "high_conf_n": len(hc), "high_conf_rate": round(hc_rate, 5) if hc_rate is not None else None,
    }


def holdout_scorecard(cur):
    """過去の発売済みを 70/30 分割し、train だけで学習→test を採点（honest なバックテスト）。"""
    cur.execute(QUERY, {"lookback": LOOKBACK_DAYS, "outcome_days": OUTCOME_DAYS})
    cols = [d[0] for d in cur.description]
    recs = cur.fetchall()

    rows = []
    for rec in recs:
        d = dict(zip(cols, rec))
        lp = d.get("launch_peak")
        if lp is None:
            continue  # 発売後CCU未観測＝結果が無い＝対にならない
        row = {name: d.get(name) for name in F.SQL_FEATURES}
        row["appid"] = d.get("appid")
        row["name"] = d.get("name")
        row["release_date"] = d.get("release_date")
        row["genres"] = _genres_list(d.get("genres"))
        row["launch_peak"] = int(lp)
        row["hit"] = int(lp) >= HIT_THRESHOLD
        rows.append(row)

    n_pairs = len(rows)
    if n_pairs == 0:
        return {"n_pairs": 0, "ready": False, "note": "対が無い（発売後CCU観測待ち）。"}

    base = sum(1 for r in rows if r["hit"]) / n_pairs
    train = [r for r in rows if M._holdout_is_train(r["appid"])]
    test = [r for r in rows if not M._holdout_is_train(r["appid"])]
    if len(train) < 50 or len(test) < 20:
        return {"n_pairs": n_pairs, "train_n": len(train), "test_n": len(test),
                "ready": False, "note": "分割後の件数が少なく honest な採点に不足（収集中）。"}

    m_tr = M.learn(train, base=base)   # ★train だけで学習（test はモデルが未知＝OOS）
    scored = []
    examples = []
    for r in test:
        s = F.score(m_tr, {k: r.get(k) for k in F.SQL_FEATURES}, r["genres"])
        p = s["prob"]
        scored.append((p, r["hit"]))
        examples.append({
            "appid": r["appid"], "name": r["name"],
            "release": _rel_iso(r["release_date"]),
            "pred": round(p, 4), "hit": bool(r["hit"]), "launch_peak": r["launch_peak"],
        })
    examples.sort(key=lambda e: e["pred"], reverse=True)

    return {
        "n_pairs": n_pairs, "train_n": len(train), "test_n": len(test), "ready": True,
        "metrics": _metrics(scored),
        "calibration": _calibration(scored),
        "examples": examples[:EXAMPLES],   # 予測が高かった作品の答え合わせ（上位）
    }


def live_scorecard(cur):
    """prediction_log（発売前に記録した予測→発売後の結果）から実運用の的中を出す。テーブルが無ければ None。"""
    cur.execute("SELECT to_regclass('public.prediction_log')")
    if cur.fetchone()[0] is None:
        return None
    cur.execute("SELECT status, count(*), count(*) FILTER (WHERE hit) FROM prediction_log GROUP BY status")
    counts = {s: {"n": n, "hits": h} for s, n, h in cur.fetchall()}
    cur.execute(
        "SELECT appid, name, release_date, spike_prob, hit, launch_peak, first_pred_at "
        "FROM prediction_log WHERE status = 'resolved' AND spike_prob IS NOT NULL "
        "ORDER BY spike_prob DESC")
    scored, examples = [], []
    for appid, name, rd, prob, hit, peak, fpa in cur.fetchall():
        scored.append((float(prob), bool(hit)))
        examples.append({
            "appid": appid, "name": name, "release": _rel_iso(rd),
            "pred": round(float(prob), 4), "hit": bool(hit),
            "launch_peak": int(peak) if peak is not None else None,
            "predicted_at": fpa.astimezone().isoformat() if fpa is not None else None,
        })
    return {
        "counts": counts,
        "pending": counts.get("pending", {}).get("n", 0),
        "settling": counts.get("settling", {}).get("n", 0),
        "resolved": counts.get("resolved", {}).get("n", 0),
        "metrics": _metrics(scored),
        "calibration": _calibration(scored) if scored else [],
        "examples": examples[:EXAMPLES],
    }


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            holdout = holdout_scorecard(cur)
            live = live_scorecard(cur)
    finally:
        conn.close()

    payload = {
        "view": "prediction_scorecard",
        "schema": "prediction_scorecard_v1",
        "note": ("羽根予想の答え合わせ（as-of・リーク無し）。holdout=過去を70/30分割しtrainだけで学習しtestを採点（honestなバックテスト）。"
                 "live=発売前に記録した予測→発売後の結果（最も硬い真のout-of-sample・日々増える）。"
                 "calibration は「◯%と予測した群が実際に何%跳ねたか」＝予測と実測が近いほど信頼できる。"),
        "generated_at": datetime.now().astimezone().isoformat(),
        "params": {"lookback_days": LOOKBACK_DAYS, "outcome_days": OUTCOME_DAYS,
                   "hit_threshold": HIT_THRESHOLD, "calib_edges": CALIB_EDGES},
        "holdout": holdout,
        "live": live,
    }

    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")

    hm = (holdout or {}).get("metrics") or {}
    print(f"書き出し: {OUT_PATH}")
    print(f"  holdout: 対 {holdout.get('n_pairs')} 件（train {holdout.get('train_n')} / test {holdout.get('test_n')}）"
          f" ready={holdout.get('ready')}")
    if hm:
        print(f"    OOS: base={hm.get('base_rate')} 上位十分位 {hm.get('top_decile_rate')}"
              f"（lift {hm.get('top_decile_lift')}）Brier {hm.get('brier')}"
              f" 高確度群 {hm.get('high_conf_rate')}（n={hm.get('high_conf_n')}）")
    if live is None:
        print("  live: prediction_log 未作成（prediction_log_sweep 未実行）＝実運用の対はこれから貯まる。")
    else:
        lm = live.get("metrics") or {}
        print(f"  live: 予測中 {live.get('pending')} / 答え合わせ待ち {live.get('settling')} / 確定 {live.get('resolved')} 件"
              f"（的中率 base={lm.get('base_rate')} 上位十分位lift {lm.get('top_decile_lift')}）")


if __name__ == "__main__":
    main()
