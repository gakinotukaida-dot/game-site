"""
発売前シグナル → 発売後の結果 の対応を貯めて“検証”する土台（読み取り専用・診断） ── 2026-07-07 / v1
================================================================
役割：「これから来そう」の **期待度（参考）** を、いつか **検証済みの「予測」** へ格上げできるように、
      発売前に観測できたシグナル（体験版CCU・最近の告知）が、実際の **発売後の跳ね** をどれだけ当てたかを
      “あと出しにならない形（as-of）”で測り、その **成績と「まだ検証に足りるデータが無い」度合い** を出す。

なぜ必要か（分析メモP3の結論の続き）：
  跳ねを断定する「予測」を名乗るには「どの前売りが当たったか」の検証基盤が要る＝それがこのバッチ。
  これが十分な対（発売前シグナルあり × 発売後の実測CCUあり）を貯め、はっきり効く（lift>1）と分かってから、
  サイトの表示を「期待度（参考）」→「検証済みの予測」に格上げする。それまでは正直に “collecting”。

線（絶対に破らない）：
- DBは読み取り専用（SELECT のみ）。書き込みは data/prelaunch_backtest.json 1ファイルだけ・毎回上書き＝可逆。
- リーク無し（as-of）：シグナルは発売日より前だけ、結果は発売日以降だけを見る。未来の値は使わない。
- 使う前売り指標は自前実測のみ（体験版CCU＝player_counts、告知の有無＝announcements）。wishlist等のグレー源は不使用。
- 著作物は載せない。出すのは件数・命中率・lift 等の集計値のみ（本文・見出しなし）。

戻し方：このファイル/ワークフローを消すだけ（data/prelaunch_backtest.json が古くなる/消えるのみ・DB無変更）。
env：LOOKBACK_DAYS / NEWS_WIN / OUTCOME_DAYS / HIT_THRESHOLD / MIN_PAIRS。
"""

import json
import os
from datetime import datetime

import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/prelaunch_backtest.json"

# 発売済みで、かつ player_counts の履歴がある見込みの範囲（収集開始以降）だけを対象にする。
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS") or "180")   # 対象：直近この日数に発売した作品
NEWS_WIN = int(os.environ.get("NEWS_WIN") or "30")             # 「発売前の告知」を数える窓（発売日から遡る日数）
OUTCOME_DAYS = int(os.environ.get("OUTCOME_DAYS") or "14")     # 「発売後の跳ね」を測る窓（発売直後の日数）
HIT_THRESHOLD = int(os.environ.get("HIT_THRESHOLD") or "1000") # 「跳ねた（hit）」とみなす発売後ピーク同時接続
MIN_PAIRS = int(os.environ.get("MIN_PAIRS") or "30")           # 検証済みへ格上げ可否の最低対数（体験版シグナル基準）

# as-of：シグナル＝発売日より前だけ、結果＝発売日以降だけ。未来を見ない＝あと出しにならない。
QUERY = """
WITH released AS (
  SELECT g.appid, g.name, g.release_date
  FROM games g
  WHERE g.release_date IS NOT NULL
    AND g.release_date <= now()::date
    AND g.release_date >= (now() - make_interval(days => %(lookback)s))::date
    AND g.coming_soon IS NOT TRUE
)
SELECT
  r.appid, r.name, r.release_date,
  -- 発売前の体験版CCU（この作品の体験版＝fullgame_appid=自分。発売日より前の最大同時接続）
  (SELECT max(pc.player_count)
     FROM player_counts pc
     JOIN games dg ON dg.appid = pc.appid AND dg.fullgame_appid = r.appid
     WHERE pc.recorded_at < r.release_date) AS demo_ccu_pre,
  -- 発売前の告知（発売日から NEWS_WIN 日前〜発売日 の間に announcements があるか。種別のみ）
  EXISTS(
    SELECT 1 FROM announcements a
    WHERE a.appid = r.appid
      AND a.published_at < r.release_date
      AND a.published_at >= r.release_date - make_interval(days => %(news_win)s)
  ) AS has_news_pre,
  -- 発売後の結果（発売日〜発売+OUTCOME_DAYS の最大同時接続＝跳ねの実測）
  (SELECT max(pc.player_count)
     FROM player_counts pc
     WHERE pc.appid = r.appid
       AND pc.recorded_at >= r.release_date
       AND pc.recorded_at < r.release_date + make_interval(days => %(outcome_days)s)) AS launch_peak
FROM released r
"""


def _rate(hits, n):
    return (hits / n) if n else None


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)  # 物理的に書けないよう固定
        with conn.cursor() as cur:
            cur.execute(QUERY, {
                "lookback": LOOKBACK_DAYS,
                "news_win": NEWS_WIN,
                "outcome_days": OUTCOME_DAYS,
            })
            recs = cur.fetchall()
    finally:
        conn.close()

    # 「対（pair）」＝発売後の実測CCUがある作品だけ（結果が測れないものは検証に使えない）。
    pairs = []
    for appid, name, rd, demo_pre, has_news_pre, launch_peak in recs:
        if launch_peak is None:
            continue  # 発売後CCU未観測＝この作品はまだ結果が無い（対にならない）
        dpre = int(demo_pre) if demo_pre is not None else 0
        pairs.append({
            "appid": appid,
            "demo_ccu_pre": dpre,
            "has_news_pre": bool(has_news_pre),
            "launch_peak": int(launch_peak),
            "hit": int(launch_peak) >= HIT_THRESHOLD,
        })

    n_pairs = len(pairs)

    def summarize(subset):
        n = len(subset)
        hits = sum(1 for p in subset if p["hit"])
        return {"n": n, "hits": hits, "hit_rate": _rate(hits, n)}

    with_demo = [p for p in pairs if p["demo_ccu_pre"] > 0]
    with_any = [p for p in pairs if p["demo_ccu_pre"] > 0 or p["has_news_pre"]]
    without_any = [p for p in pairs if not (p["demo_ccu_pre"] > 0 or p["has_news_pre"])]

    s_demo = summarize(with_demo)
    s_with = summarize(with_any)
    s_without = summarize(without_any)

    # lift＝「シグナルあり」の命中率が「シグナルなし」の何倍か（>1 で効いている）。両方に件数が要る。
    lift = None
    if s_with["hit_rate"] is not None and s_without["hit_rate"]:
        lift = round(s_with["hit_rate"] / s_without["hit_rate"], 2)

    # 格上げ判定：体験版シグナルの対が MIN_PAIRS 以上あり、かつ lift>1 が見えてから「検証済み予測」へ。
    #   それまでは正直に "collecting"（＝サイトは「期待度（参考）」のまま）。
    ready = (s_demo["n"] >= MIN_PAIRS) and (lift is not None and lift > 1.0)
    readiness = "validated" if ready else "collecting"
    need_more = max(0, MIN_PAIRS - s_demo["n"])

    payload = {
        "view": "prelaunch_backtest",
        "schema": "prelaunch_backtest_v1",
        "note": ("発売前シグナル→発売後の跳ね の as-of 検証（リーク無し）。readiness=collecting の間は"
                 "サイトは「期待度（参考）」のまま。validated になったら「検証済み予測」へ格上げ可。"),
        "generated_at": datetime.now().astimezone().isoformat(),
        "params": {
            "lookback_days": LOOKBACK_DAYS, "news_win": NEWS_WIN,
            "outcome_days": OUTCOME_DAYS, "hit_threshold": HIT_THRESHOLD, "min_pairs": MIN_PAIRS,
        },
        "readiness": readiness,               # collecting / validated
        "need_more_demo_pairs": need_more,    # あと何対で格上げ判定に届くか（体験版基準）
        "n_released_scanned": len(recs),
        "n_pairs": n_pairs,                   # 発売後CCUまで揃った対の数
        "signal_demo": s_demo,                # 体験版CCUありの対（最も硬いシグナル）
        "signal_any": s_with,                 # 体験版CCU or 告知ありの対
        "no_signal": s_without,               # シグナル無しの対（対照群）
        "lift_any_vs_none": lift,             # シグナルあり/なし の命中率比
    }

    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")

    print(f"書き出し: {OUT_PATH}")
    print(f"  発売済スキャン {len(recs)} 件 / 対（発売後CCUあり）{n_pairs} 件")
    print(f"  体験版シグナルあり {s_demo['n']} 件（命中 {s_demo['hits']}・率 {s_demo['hit_rate']}）")
    print(f"  何かシグナルあり {s_with['n']} 件（率 {s_with['hit_rate']}） / なし {s_without['n']} 件（率 {s_without['hit_rate']}）")
    print(f"  lift（あり/なし）= {lift}")
    print(f"  readiness = {readiness}（体験版対 あと {need_more} で MIN_PAIRS={MIN_PAIRS}）")
    if readiness == "collecting":
        print("  → まだ検証に足りるデータが無い＝サイトは「期待度（参考）」のまま（正直）。データが貯まれば自動で近づく。")


if __name__ == "__main__":
    main()
