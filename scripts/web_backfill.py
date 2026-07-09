#!/usr/bin/env python3
"""
web_views の“発売前ページビュー”を過去分バックフィル（羽根予想モデルの web 特徴を自動で本活性化する）── 2026-07-08 / v1
================================================================
なぜ必要か（本活性化のカギ）：
  web_mentions の収集は最近始まったため、現在の学習対象（直近180日に発売済み）は
  「発売前の web 履歴」を持たない＝web 特徴量が全部 none＝重みゼロのまま何ヶ月も眠ってしまう。
  そこで **Wikipedia Pageviews API が過去日付も引ける**性質を使い、各作品の発売前2週間の
  実閲覧（全言語合計）を **as-of で再構成**し、recorded_at＝発売日前日 で web_mentions に記録する。
  → 次回のモデル再学習（毎日04:20）が発売前 web_views を特徴量として拾い、重みが自動で付き始める＝本活性化。

as-of の正しさ（リーク無し）：
  記録する値は [発売日-14日, 発売日] の実閲覧＝**発売前の情報のみ**。recorded_at を発売日前日にするので、
  学習クエリ（feature_sql の wm.recorded_at < release_date）が正しく“発売前スナップショット”として拾う。
  ※ backfill するのは web_views（真に過去を引ける実閲覧）だけ。web_news(GDELT)/web_reach(現在値) は
    過去再構成が不正確/現在情報の混入になるため入れない（前向きの日次収集で自然に活性化）。

線（絶対に破らない）：
- 分離テーブル web_mentions のみ INSERT。既存資産に無影響・可逆（消せば web_views の過去分が消えるだけ）。
- 冪等：発売前 web_views 行を既に持つ作品はスキップ（0件でも記録して“処理済み”にする）＝再実行で少しずつ全体を覆う。
- 成人向けは対象外。著作物は載せない（appid・source・数値のみ）。
- Neon の scale-to-zero 復帰時の一時 read-only(25006) に指数バックオフ再試行。
env：LOOKBACK_DAYS / PAGEVIEW_DAYS / BACKFILL_CAP / MAX_PV_LANGS / WRITE_RETRIES。
"""
import os
import sys
import time
from datetime import timedelta

import psycopg2
from psycopg2.extras import execute_batch

from _filters import not_adult
from web_mentions_sweep import DDL, pageviews_between  # テーブル定義と“任意期間ページビュー合計”を共有

DATABASE_URL = os.environ["DATABASE_URL"]
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS") or "180")   # 学習窓と一致（この範囲の発売済みを覆う）
PAGEVIEW_DAYS = int(os.environ.get("PAGEVIEW_DAYS") or "14")    # 発売前の実閲覧を測る窓（日次収集と一致）
BACKFILL_CAP = int(os.environ.get("BACKFILL_CAP") or "100")    # 1回の処理件数（言語数ぶんAPIを叩く。加速のため増量・10件ごと書込で安全）
FLUSH_EVERY = int(os.environ.get("FLUSH_EVERY") or "10")       # この件数ごとに書き込む＝計算(HTTP)中も write-primary を温存し cold-start を回避

# 発売済み（直近LOOKBACK日）で、まだ“発売前 web_views”を持たない作品。発売日が新しい順に少しずつ。
TARGET_QUERY = f"""
SELECT g.appid, g.name, g.release_date
FROM games g
WHERE g.release_date IS NOT NULL
  AND g.release_date <= now()::date
  AND g.release_date >= (now() - make_interval(days => %(lookback)s))::date
  AND g.coming_soon IS NOT TRUE
  AND g.name IS NOT NULL AND char_length(g.name) >= 3
  AND {not_adult('g')}
  AND NOT EXISTS (
    SELECT 1 FROM web_mentions wm
    WHERE wm.appid = g.appid AND wm.source = 'wikipedia_pageviews'
      AND wm.recorded_at < g.release_date
  )
ORDER BY g.release_date DESC
LIMIT %(cap)s
"""

# recorded_at＝発売日前日（as-of で“発売前”に入るように）。source は日次収集と同一キー。
INSERT_SQL = ("INSERT INTO web_mentions (appid, source, mentions, recorded_at) "
              "VALUES (%s, 'wikipedia_pageviews', %s, %s)")


def _connect_retry(fn):
    """接続して fn(conn) を実行。Neon cold-start の一時 read-only(25006)/接続断に指数バックオフ再試行。"""
    attempts = int(os.environ.get("WRITE_RETRIES") or "9")
    for i in range(attempts):
        conn = None
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            r = fn(conn)
            conn.commit()
            return r
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


def get_targets():
    """READ 専用で対象を取得（先に読んで compute を起こす）。テーブルが有る前提（無い場合は get_targets_or_none 経由）。"""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(TARGET_QUERY, {"lookback": LOOKBACK_DAYS, "cap": BACKFILL_CAP})
            return cur.fetchall()
    finally:
        conn.close()


def get_targets_or_none():
    """READ 専用。web_mentions が無ければ None（＝要 ensure＋全件）。有れば未処理対象（空リストなら完了済み）を返す。
    ★これで「完了済み」の頻繁な定時実行を **書き込みゼロ（READのみ）** で即終了でき、no-op ランの cold-start 失敗を出さない。"""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.web_mentions')")
            if cur.fetchone()[0] is None:
                return None
            cur.execute(TARGET_QUERY, {"lookback": LOOKBACK_DAYS, "cap": BACKFILL_CAP})
            return cur.fetchall()
    finally:
        conn.close()


def _flush(rows):
    """rows を一括 INSERT（recorded_at＝発売日前日）。書き込み＝Neon cold-start に再試行つき。"""
    if rows:
        _connect_retry(lambda conn: execute_batch(conn.cursor(), INSERT_SQL, rows, page_size=200))


def main():
    # 1) 読み取り優先：テーブルが有り未処理が無ければ「書き込みゼロ」で即終了（頻繁な定時実行でも cold-start 失敗を出さない）。
    targets = get_targets_or_none()
    if targets is None:
        # テーブル未作成（初回）＝作成してから対象取得。書き込み＝再試行つき。
        _connect_retry(lambda conn: conn.cursor().execute(DDL))
        try:
            targets = get_targets()
        except psycopg2.Error as e:
            print(f"[web-backfill] 対象取得に失敗（{getattr(e, 'pgcode', '')}）→ 今回はスキップ。")
            return 0
    if not targets:
        print("[web-backfill] 発売前 web_views 未取得の対象なし＝バックフィル完了済み（または対象0）。")
        return 0

    # 3) 各作品の発売前 [release_date-PAGEVIEW_DAYS, release_date] の全言語ページビュー合計を計算し、
    #    FLUSH_EVERY 件ごとに書き込む。★計算(HTTP)は数分かかるため、こまめに書いて write-primary を温存
    #    ＝長い無書き込み時間で compute が scale-to-zero → 最後の一括書き込みが cold-start で落ちる、を防ぐ。
    buffer = []
    written = 0
    for appid, name, rd in targets:
        start = rd - timedelta(days=PAGEVIEW_DAYS)
        try:
            total = int(pageviews_between(name, start, rd))
        except Exception as e:   # 1作品の失敗で全体を止めない（次回に再挑戦）
            print(f"  [skip] appid={appid} '{(name or '')[:28]}': {type(e).__name__}")
            continue
        rec_at = rd - timedelta(days=1)   # 発売日前日＝as-of で“発売前”に入る
        buffer.append((appid, total, rec_at))
        print(f"  web_views(発売前) appid={appid:>9} {total:>9}  発売 {rd}  {(name or '')[:34]}")
        if len(buffer) >= FLUSH_EVERY:
            _flush(buffer)
            written += len(buffer)
            buffer = []
    _flush(buffer)
    written += len(buffer)

    if not written:
        print("[web-backfill] 記録行 0（全件失敗/該当なし）。")
        return 0
    print(f"[web-backfill] 記録 {written} 件（発売前 web_views・recorded_at=発売日前日・{FLUSH_EVERY}件ごとに書込）。"
          f"次回のモデル再学習(04:20)が特徴量として拾い、web_views の重みが自動で付き始めます。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
