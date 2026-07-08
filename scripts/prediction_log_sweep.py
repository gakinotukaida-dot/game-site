#!/usr/bin/env python3
"""
羽根予想の“実運用トラッキング”── 予測を保存し、発売後に答え合わせする（分離テーブル・追記/更新のみ）── 2026-07-08 / v1
================================================================
役割（このサイトの信頼の核）：発売前に出した羽根予想（跳ね確率）を **その時点で記録** し、
  発売して結果（発売直後の最大同時接続）が出そろったら **跳ねたか否かを判定** して凍結する。
  こうして貯まった「予測 → 結果」の対が、公開の“的中率スコアカード”（export_scorecard.py）の
  最も硬い証拠＝真の out-of-sample（予測時点で結果も学習データも未知）になる。

なぜ DB に貯めるか（git の upcoming.json 履歴があるのに）：
  結果との突き合わせ済みの「確定した対」を、あと出し無く・機械的に集計できる形で残すため。
  記録する予測値は export_upcoming.compute_rows と **同一の計算**（＝サイト表示と1ビットも違わない。skew防止）。

判定の定義（prelaunch_model と厳密に一致）：
  hit = 発売日〜発売+OUTCOME_DAYS の最大同時接続 >= HIT_THRESHOLD。
  結果は発売直後の窓が閉じてから（release_date+OUTCOME_DAYS <= now）確定＝安定値だけを resolved にする。

線（絶対に破らない）：
- 分離テーブル prediction_log だけを作成・INSERT/UPDATE のみ＝既存資産に無影響・可逆（消せば記録が止まるだけ）。
- 予測は「発売前」の作品にだけ記録（compute_rows が返す＝発売前のみ）。発売済みは対象から外れ、最後の発売前値が自然に凍結。
- 成人向けは対象外（compute_rows の UPCOMING_WHERE が not_adult を含む）。
- Neon の scale-to-zero 復帰時の一時 read-only(25006)/接続断に指数バックオフ再試行。
env：PRED_LIMIT（1回に見る発売前作品の上限・既定800）/ OUTCOME_DAYS / HIT_THRESHOLD / WRITE_RETRIES。
"""
import os
import sys
import time

import psycopg2
from psycopg2.extras import execute_values

import export_upcoming as EU   # 予測の単一の源（compute_rows）を共有＝表示と記録を一致させる

DATABASE_URL = os.environ["DATABASE_URL"]
PRED_LIMIT = int(os.environ.get("PRED_LIMIT") or "800")        # 1回に記録する発売前作品の上限
OUTCOME_DAYS = int(os.environ.get("OUTCOME_DAYS") or "14")     # 発売直後の跳ねを測る窓（model と一致）
HIT_THRESHOLD = int(os.environ.get("HIT_THRESHOLD") or "1000") # 跳ね（hit）とみなす発売後ピーク（model と一致）

DDL = """
CREATE TABLE IF NOT EXISTS prediction_log (
  appid            bigint PRIMARY KEY,
  name             text,
  release_date     date,
  first_pred_at    timestamptz NOT NULL DEFAULT now(),  -- 最初に予測を記録した時刻（不変）
  last_pred_at     timestamptz NOT NULL DEFAULT now(),  -- 最後に発売前予測を更新した時刻
  spike_prob       double precision,                    -- 記録した跳ね確率（最後の発売前値＝凍結対象）
  expect           text,
  conf             text,
  model_readiness  text,
  model_gen_at     text,
  status           text NOT NULL DEFAULT 'pending',     -- pending(発売前) / settling(発売直後・窓が閉じる前) / resolved(確定)
  launch_peak      integer,                             -- 発売直後の最大同時接続（確定時に記録）
  hit              boolean,                             -- 跳ねたか（launch_peak>=HIT_THRESHOLD）
  resolved_at      timestamptz
)
"""

UPSERT = """
INSERT INTO prediction_log
  (appid, name, release_date, spike_prob, expect, conf, model_readiness, model_gen_at, status, first_pred_at, last_pred_at)
VALUES %s
ON CONFLICT (appid) DO UPDATE SET
  name            = EXCLUDED.name,
  release_date    = EXCLUDED.release_date,
  spike_prob      = EXCLUDED.spike_prob,
  expect          = EXCLUDED.expect,
  conf            = EXCLUDED.conf,
  model_readiness = EXCLUDED.model_readiness,
  model_gen_at    = EXCLUDED.model_gen_at,
  last_pred_at    = now(),
  status          = 'pending'
WHERE prediction_log.status <> 'resolved'
"""
# ↑ first_pred_at は挿入時のみ（ON CONFLICT で触らない）＝“最初の予測時刻”を保つ。
#   resolved 済みは二度と上書きしない（万一 coming_soon 再点灯でも確定を守る）。

# 発売直後の窓が閉じた作品を確定（as-of と同義：発売日〜発売+OUTCOME_DAYS の最大CCU）。
RESOLVE = """
UPDATE prediction_log pl
SET launch_peak = sub.peak,
    hit         = (sub.peak >= %(hit)s),
    status      = 'resolved',
    resolved_at = now()
FROM (
  SELECT pl2.appid, COALESCE(max(pc.player_count), 0) AS peak
  FROM prediction_log pl2
  LEFT JOIN player_counts pc
    ON pc.appid = pl2.appid
   AND pc.recorded_at >= pl2.release_date
   AND pc.recorded_at <  pl2.release_date + make_interval(days => %(outcome)s)
  WHERE pl2.status <> 'resolved'
    AND pl2.release_date IS NOT NULL
    AND pl2.release_date + make_interval(days => %(outcome)s) <= now()
  GROUP BY pl2.appid
) sub
WHERE pl.appid = sub.appid
"""

# 発売はしたが窓がまだ閉じていない作品を settling に（表示用の“答え合わせ待ち”）。
SETTLING = """
UPDATE prediction_log
SET status = 'settling'
WHERE status = 'pending'
  AND release_date IS NOT NULL
  AND release_date <= now()::date
  AND release_date + make_interval(days => %(outcome)s) > now()
"""


def _run(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
        # 1) 発売前の予測を記録（表示と同一の compute_rows。DBセッションは呼び出し側=書き込み可のまま）。
        rows, model, _base, _validated = EU.compute_rows(conn, limit=PRED_LIMIT)
        logged = 0
        if model is None:
            print("[pred-log] モデルが無い＝予測できないので記録スキップ（data/prelaunch_model.json 未生成）。")
        else:
            values = []
            for r in rows:
                if r.get("spike_prob") is None:
                    continue  # 確率が出せない作品は記録しない（材料/モデル不足）
                rd = r.get("release") if r.get("release_known") else None
                values.append((
                    r["appid"], (r.get("name") or "")[:200], rd,
                    r["spike_prob"], r.get("expect"), r.get("conf"),
                    model.get("readiness"), model.get("generated_at"),
                ))
            if values:
                execute_values(
                    cur, UPSERT, values,
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,'pending',now(),now())",
                )
                logged = len(values)

        # 2) 発売直後の窓が閉じた作品を確定（答え合わせ）。
        cur.execute(RESOLVE, {"hit": HIT_THRESHOLD, "outcome": OUTCOME_DAYS})
        resolved = cur.rowcount
        # 3) 発売済み・窓が閉じる前を settling に。
        cur.execute(SETTLING, {"outcome": OUTCOME_DAYS})
        settling = cur.rowcount

        # 集計（表示ログ用）。
        cur.execute("SELECT status, count(*), count(*) FILTER (WHERE hit) FROM prediction_log GROUP BY status")
        by_status = {s: (n, h) for s, n, h in cur.fetchall()}
    conn.commit()
    return logged, resolved, settling, by_status


def main():
    attempts = int(os.environ.get("WRITE_RETRIES") or "9")
    for i in range(attempts):
        conn = None
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            logged, resolved, settling, by_status = _run(conn)
            tot_res = by_status.get("resolved", (0, 0))
            print(f"[pred-log] 記録 {logged} 件・今回確定 {resolved} 件・settling {settling} 件")
            print(f"           累計: pending={by_status.get('pending', (0,0))[0]} "
                  f"settling={by_status.get('settling', (0,0))[0]} "
                  f"resolved={tot_res[0]}（うち跳ね {tot_res[1]}）")
            return 0
        except psycopg2.Error as e:
            if conn is not None:
                conn.rollback()
            transient = (getattr(e, "pgcode", None) == "25006") or isinstance(e, psycopg2.OperationalError)
            if not transient or i == attempts - 1:
                raise
            wait = min(30, 3 * (2 ** i))
            print(f"[retry] 一時 read-only/接続断（{getattr(e, 'pgcode', '')}）→ {wait}s 後に再試行 ({i+1}/{attempts})")
            time.sleep(wait)
        finally:
            if conn is not None:
                conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
