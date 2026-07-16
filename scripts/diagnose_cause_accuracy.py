#!/usr/bin/env python3
# 調査精度の診断（D・プロセス指標）── 読み取り専用・DB非依存・鍵不要 ── 2026-07-16
# ================================================================
# 入力: data/view02_rising.json（view02_rank_v2 の出力。B で付与した investigation ブロックを利用）
# 出力: data/cause_scorecard.json（カバレッジ／調査中の内訳／Web調査の効き／ソース別点灯／確信度分布）
#
# 目的（当初の狙い「調査中をちゃんと調査できているか」を測る物差し）:
#   ここで測るのは “跳ね正解” ではなく **調査プロセスの健全性**（confound の無いプロセス指標）。
#   ・unknown_rate      = 調査中 / 全件（減っているほど良い、ただし誤ラベルを増やしていないか §D と併読）
#   ・unknown_reasons   = 「調べ尽くして陰性(investigated_all_negative)＝正直な不明」対
#                          「予算で未照会(web_skipped_budget)＝カバレッジ欠落」の内訳。
#                          後者が多い＝WEB_MAX_QUERIES 等を緩める余地がある、という運用の手掛かり。
#   ・web_investigation = GDELTを照会した件数・点灯件数・Web単独で解明した件数（PR#39 の効き）。
#   ・signal_hits       = 各きっかけソースの点灯件数（1作品に複数可）。どのソースが効いているかの全体像。
#   ・confidence        = high/mid/low の分布。
#
# 立ち位置（過大主張しない）: これは帰属の“正しさ”の証明ではない。カバレッジと調査の網羅性を可視化する
#   プロセス計測であり、原因別の前方持続リフト（帰属の識別価値）は履歴が貯まってから別途評価する（Phase2）。
#
# 実行: ローカル `python scripts/diagnose_cause_accuracy.py`
#       CI は .github/workflows/cause_scorecard.yml（workflow_dispatch・手動）。
import json
import os
import sys
import datetime

SRC = os.environ.get("SCORECARD_SRC") or "data/view02_rising.json"
OUT = os.environ.get("SCORECARD_OUT") or "data/cause_scorecard.json"


def main():
    try:
        d = json.load(open(SRC, encoding="utf-8"))
    except Exception as e:
        print(f"[cause-scorecard] {SRC} を読めません（スキップ）: {e}", file=sys.stderr)
        return 0

    items = d.get("items") or []
    n = len(items)

    known = unknown = 0
    reasons = {}                 # 調査中の内訳（investigated_all_negative / web_skipped_budget / web_query_failed / unlabeled）
    web_queried = web_hit = web_solo = web_err = 0
    signal_hits = {}             # きっかけソース別の点灯件数（1作品に複数可）
    conf = {}
    has_inv = False

    for it in items:
        sigs = it.get("signals") or []
        types = [s.get("type") for s in sigs]
        for t in types:
            signal_hits[t] = signal_hits.get(t, 0) + 1
        conf_code = it.get("confidence")
        conf[conf_code] = conf.get(conf_code, 0) + 1

        is_known = bool((it.get("prediction") or {}).get("known", bool(sigs)))
        if is_known:
            known += 1
        else:
            unknown += 1

        inv = it.get("investigation") or {}
        if inv:
            has_inv = True
        res = inv.get("results") or {}
        web = res.get("web") or {}
        if web.get("queried"):
            web_queried += 1
        if web.get("hit"):
            web_hit += 1
        if web.get("error"):
            web_err += 1
        if types == ["web_buzz"]:     # 他ソース陰性で Web 単独が解明した＝PR#39 の直接効果
            web_solo += 1

        if not is_known:
            key = inv.get("unknown_reason") or "unlabeled"
            reasons[key] = reasons.get(key, 0) + 1

    scorecard = {
        "meta": {
            "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": SRC,
            "source_generated_at": (d.get("meta") or {}).get("generated_at"),
            "item_count": n,
            "has_investigation_meta": has_inv,   # False＝旧JSON/INV_META=OFF。プロセス指標は限定的になる。
            "note": "process_metrics_only_not_attribution_truth",
        },
        "coverage": {
            "known": known,
            "unknown": unknown,
            "unknown_rate": round(unknown / n, 4) if n else None,
            "unknown_reasons": dict(sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)),
        },
        "web_investigation": {
            "queried": web_queried,
            "hit": web_hit,
            "resolved_solo": web_solo,   # Web単独で「調査中」→解明にした件数
            "query_failed": web_err,     # 照会失敗（レート制限等・陰性と区別＝調査の網羅性の欠け）
            "hit_rate": round(web_hit / web_queried, 4) if web_queried else None,
        },
        "signal_hits": dict(sorted(signal_hits.items(), key=lambda kv: kv[1], reverse=True)),
        "confidence": conf,
    }

    # ---- 人が読む要約 ----
    print("=" * 72)
    print(f"調査スコアカード（プロセス指標）  source={SRC}")
    print(f"  対象 {n} 件 / 既知 {known} ・ 調査中 {unknown}"
          + (f"（{scorecard['coverage']['unknown_rate']:.1%}）" if n else ""))
    if not has_inv:
        print("  ⚠ investigation メタ無し（旧JSON か INV_META=OFF）。内訳・Web指標は限定的。")
    if reasons:
        parts = " / ".join(f"{k}={v}" for k, v in scorecard["coverage"]["unknown_reasons"].items())
        print(f"  調査中の内訳: {parts}")
    print(f"  Web調査: 照会 {web_queried} ・ 点灯 {web_hit} ・ Web単独解明 {web_solo} ・ 照会失敗 {web_err}"
          + (f" ・ 点灯率 {scorecard['web_investigation']['hit_rate']:.1%}" if web_queried else ""))
    if signal_hits:
        top = " / ".join(f"{k}:{v}" for k, v in list(scorecard["signal_hits"].items())[:8])
        print(f"  ソース別点灯: {top}")
    print("=" * 72)

    try:
        out_dir = os.path.dirname(OUT)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(OUT, "w", encoding="utf-8") as f:
            json.dump(scorecard, f, ensure_ascii=False, indent=2)
        print(f"出力: {OUT}")
    except Exception as e:
        print(f"  ⚠ JSON書き出し失敗: {type(e).__name__}: {e}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
