#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
B2 目利きスコア データ準備度 診断（E1・読み取り専用・印字のみ・本番非変更）
置き場所: scripts/scout_diag.py

目的（案2_目利きスコア仕様 §7 / 蓄積開始）:
  配信者が「跳ねる前の小型/新作に乗り、その後 実際に CCU が伸びた」度合い（目利き=scout）を、
  データから学習する前に、**いま計算できるだけの資料があるか**を実測する（diagnose_review_history と同型）。
  これ1回で『pickupの量・解決済みイベント数・p0（基準率）・上位scoutのlift』が分かる。データ不足なら正直に0件と出る。

  リーク防止（仕様§3）を厳守：baseline_at_t は ≤t のみ、outcome は (t, t+H] のみ、採点対象は解決済み(t ≤ now-H)のみ。
  outcome は Steam CCU（配信視聴でなく実プレイヤー＝循環回避）。

  read-only（SELECTのみ・物理的に書けない固定）。書き込み・ファイル変更・commit は一切しない。
  出力を Claude に共有 → 資料が足りれば派生統計テーブル(user_id単位・最小・減衰)の設計へ。足りなければ蓄積待ち。

前提スキーマ（確定）:
  streamer_activity(twitch_user_id, login, appid, viewer_count, recorded_at, ... 14日ローリング)
  player_counts(appid, player_count, recorded_at)
守る線: 個人は user_id 単位の集計のみ・login は出さない（B2弁護士スコープ・最小）。
"""
import os
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]

H_DAYS      = int(os.environ.get("H_DAYS")      or "14")    # 前向き窓（pickup後この日数で hit を測る）
BASE_DAYS   = int(os.environ.get("BASE_DAYS")   or "14")    # baseline_at_t の窓（t以前）
SMALL_CAP   = int(os.environ.get("SMALL_CAP")   or "5000")  # eligibility：t時点で小型（baseline<=これ）
RISE_MULT   = float(os.environ.get("RISE_MULT") or "2.0")   # hit：窓内 max CCU >= RISE_MULT×baseline_at_t
MIN_BASE_PTS = int(os.environ.get("MIN_BASE_PTS") or "3")   # baseline の最低点数
N_MIN       = int(os.environ.get("N_MIN")       or "5")     # scout採点の最低解決pickup数
K_PRIOR     = float(os.environ.get("K_PRIOR")   or "20")    # 収縮の事前の強さ（経験ベイズ）
EXAMPLES    = int(os.environ.get("EXAMPLES")    or "12")


def _pct(a, b):
    return "—" if not b else f"{100.0 * a / b:.1f}%"


def main():
    print("=" * 76)
    print("B2 目利きスコア データ準備度 診断（読み取り専用・印字のみ・本番非変更）")
    print(f"params: H_DAYS={H_DAYS} BASE_DAYS={BASE_DAYS} SMALL_CAP={SMALL_CAP} RISE_MULT={RISE_MULT} "
          f"MIN_BASE_PTS={MIN_BASE_PTS} N_MIN={N_MIN} K_PRIOR={K_PRIOR}")
    print("=" * 76)

    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:

            # ---------- [1] streamer_activity の量・鮮度 ----------
            try:
                cur.execute("SELECT min(recorded_at), max(recorded_at), count(*), "
                            "count(DISTINCT twitch_user_id), count(DISTINCT appid), "
                            "count(DISTINCT (twitch_user_id, appid)) FROM streamer_activity")
                mn, mx, total, n_user, n_app, n_pair = cur.fetchone()
                span = (mx - mn).days if (mn and mx) else None
                print("\n[1] streamer_activity の量・鮮度")
                print(f"  期間: {mn} 〜 {mx}（約 {span} 日ぶん）")
                print(f"  総行数={total}  配信者(user)数={n_user}  ゲーム(appid)数={n_app}  pickup(user×game)組={n_pair}")
                print(f"  ※ 解決済み pickup（t ≤ now-{H_DAYS}日）だけが採点対象。14日ローリング収集だと解決分は薄い＝正常。")
            except Exception as e:
                print(f"⚠ [1] 失敗: {type(e).__name__}: {e}")

            # ---------- pickup（(user,game) の初観測時刻 t）＋ baseline_at_t（≤t）＋ 解決フラグ ----------
            # リーク防止：baseline は t 以前のみ。hit は (t, t+H] のみ。採点は t ≤ now-H のみ。
            PICK_SQL = """
            WITH pick AS (
              SELECT twitch_user_id AS uid, appid, min(recorded_at) AS t
              FROM streamer_activity GROUP BY twitch_user_id, appid
            ),
            base AS (
              SELECT p.uid, p.appid, p.t,
                (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY pc.player_count)
                   FROM player_counts pc
                   WHERE pc.appid = p.appid
                     AND pc.recorded_at <  p.t
                     AND pc.recorded_at >= p.t - make_interval(days => %(base)s)) AS baseline_at_t,
                (SELECT count(*) FROM player_counts pc
                   WHERE pc.appid = p.appid AND pc.recorded_at < p.t
                     AND pc.recorded_at >= p.t - make_interval(days => %(base)s)) AS base_n,
                (SELECT max(pc.player_count) FROM player_counts pc
                   WHERE pc.appid = p.appid AND pc.recorded_at > p.t
                     AND pc.recorded_at <= p.t + make_interval(days => %(h)s)) AS fwd_max,
                (p.t <= now() - make_interval(days => %(h)s)) AS resolved
              FROM pick p
            )
            SELECT uid, appid, t, baseline_at_t, base_n, fwd_max, resolved FROM base
            """
            rows = []
            try:
                cur.execute(PICK_SQL, {"base": BASE_DAYS, "h": H_DAYS})
                rows = cur.fetchall()
            except Exception as e:
                print(f"⚠ pickup/baseline 取得 失敗: {type(e).__name__}: {e}")

            # ---------- [2] eligibility / resolved の絞り込み ----------
            total_pick = len(rows)
            resolved = [r for r in rows if r[6]]
            # eligible = 解決済み かつ baseline が測れて 小型（跳ねる前に乗った）
            elig = [r for r in resolved
                    if r[3] is not None and (r[4] or 0) >= MIN_BASE_PTS and r[3] <= SMALL_CAP]
            print("\n[2] 採点対象の絞り込み（リーク防止済み）")
            print(f"  全 pickup={total_pick}  解決済み(t≤now-{H_DAYS}日)={len(resolved)}  "
                  f"eligible(解決＋baseline有＋小型≤{SMALL_CAP})={len(elig)}")
            if not elig:
                print(f"  → eligible 0件＝いまは採点できない（14日ローリング＋H={H_DAYS}日で解決分が薄い＝仕様どおり）。")
                print("     蓄積が H＋数か月ぶん貯まってから再診断（案2_目利きスコア仕様 §7）。作る前に測る＝正しい。")
                print("\n" + "=" * 76)
                print("この出力を Claude に共有してください。eligible>0 になったら派生統計(lift)の設計に進みます。")
                print("書き込み・ファイル変更・commit は一切していません（read-only固定）。")
                return

            # ---------- [3] p0（基準率）＋ scout 別 lift ----------
            def is_hit(r):
                bl, fmax = r[3], r[5]
                return (fmax is not None and bl and fmax >= RISE_MULT * bl)
            hits = sum(1 for r in elig if is_hit(r))
            p0 = hits / len(elig)
            print(f"\n[3] 基準率と目利き（scout）lift")
            print(f"  p0（eligible全体の hit率）= {hits}/{len(elig)}（{_pct(hits, len(elig))}）"
                  f"  ＝小型が {H_DAYS}日で ×{RISE_MULT} 伸びる素の確率")
            # scout 別に収縮（経験ベイズ）：p̂ = (h+α)/(n+α+β), α=p0·K, β=(1-p0)·K
            from collections import defaultdict
            n_s = defaultdict(int); h_s = defaultdict(int)
            for r in elig:
                n_s[r[0]] += 1; h_s[r[0]] += 1 if is_hit(r) else 0
            alpha = p0 * K_PRIOR; beta = (1 - p0) * K_PRIOR
            scored = []
            for uid, n in n_s.items():
                if n < N_MIN:
                    continue
                phat = (h_s[uid] + alpha) / (n + alpha + beta)
                lift = phat / p0 if p0 > 0 else None
                scored.append((uid, n, h_s[uid], phat, lift))
            scored.sort(key=lambda x: (x[4] or 0), reverse=True)
            print(f"  n≥{N_MIN} の scout（採点可能）= {len(scored)} 人")
            if scored:
                print("  ── lift 上位（user_id は集計のみ・login は出さない＝守る線）")
                print("     {:<12} {:>4} {:>4} {:>7} {:>6}".format("user_id", "n", "hit", "p̂", "lift"))
                for uid, n, h, phat, lift in scored[:EXAMPLES]:
                    print("     {:<12} {:>4} {:>4} {:>7} {:>6}".format(
                        str(uid)[:12], n, h, f"{phat:.2f}", f"{lift:.2f}" if lift is not None else "—"))
                print("  ※ lift>1 が安定して出る scout が居れば、B2予兆レーンに上限付きで投入（実験表示）。")
            else:
                print(f"  → n≥{N_MIN} の scout が居ない＝まだ薄い。蓄積待ち。")

    finally:
        conn.close()

    print("\n" + "=" * 76)
    print("この出力を Claude に共有してください。p0/lift が意味を持つ量になったら派生統計テーブルの設計へ。")
    print("書き込み・ファイル変更・commit は一切していません（read-only固定）。")


if __name__ == "__main__":
    main()
