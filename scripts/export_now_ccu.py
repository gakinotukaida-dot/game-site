"""
表示用エクスポート（候補1「今のプレイヤー数」）── 2026-06-06
================================================================
役割：Neon を「読むだけ」で、各appidの最新CCU上位20件を取り、
      表示の箱(radar_shell) view01 が読める JSON を data/now_ccu.json に書き出す。

設計の線（土台・確定事項と整合）：
- DBは読み取り専用（SELECT のみ）。書き込み・スキーマ変更は一切しない＝(B)・非破壊。
- STEAM_API_KEY は不要（DBを読むだけ）。env は DATABASE_URL のみ。
- 出力は「データだけで決まる」内容（observed_at と rows）にする＝中身が変わらなければ
  ファイルはバイト同一→ワークフロー側で「変更時のみコミット」が効く（best-effort二重発火の無駄コミット回避）。
  ※ 生成時刻(wall clock)はファイルに入れない（毎回変わってしまい差分が常に出るため）。
- クエリは sql/candidate1_now_ccu.sql と同一（各appid最新→CCU降順→上位20→games で命名）。

戻し方：このファイルと .github/workflows/export.yml と data/now_ccu.json を削除すれば元に戻る（可逆）。
"""

import json
import os
from datetime import datetime, timezone

import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/now_ccu.json"
TOP_N = int(os.environ.get("TOP_N") or "20")  # 上位件数（箱の view01 は20件想定）。暫定・env可変。

# 候補1クエリ（sql/candidate1_now_ccu.sql と同一の考え方）。
# 各appidの最新記録(DISTINCT ON)→ CCU降順 上位N → games で名前付与。返りは JSON 配列1セル。
QUERY = """
WITH latest AS (
  SELECT DISTINCT ON (appid) appid, player_count, recorded_at
  FROM player_counts
  ORDER BY appid, recorded_at DESC
)
SELECT json_agg(
         json_build_object('appid', l.appid, 'name', g.name,
                           'ccu', l.player_count, 'observed_at', l.recorded_at)
         ORDER BY l.player_count DESC
       ) AS now_list
FROM (SELECT * FROM latest ORDER BY player_count DESC LIMIT %s) l
JOIN games g ON g.appid = l.appid;
"""


def _as_list(cell):
    """psycopg2 は json を Python オブジェクトで返すことも文字列で返すこともある。両対応。"""
    if cell is None:
        return []
    if isinstance(cell, (list, tuple)):
        return list(cell)
    if isinstance(cell, str):
        return json.loads(cell)
    return list(cell)


def _max_observed_at(items):
    """rows の observed_at(ISO文字列)の最大を返す。観測時刻の代表値（最新）。"""
    best = None
    for it in items:
        raw = it.get("observed_at")
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if best is None or dt > best:
            best = dt
    return best.isoformat() if best else None


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:  # 読み取りのみ（commit しない）
            cur.execute(QUERY, (TOP_N,))
            cell = cur.fetchone()[0]
    finally:
        conn.close()

    items = _as_list(cell)
    observed_at = _max_observed_at(items)

    # 箱(view01)が必要とする最小の形 {appid, name, ccu} に絞る（観測時刻は top-level の observed_at に集約）。
    rows = [
        {"appid": it["appid"], "name": it.get("name"), "ccu": it["ccu"]}
        for it in items
    ]

    payload = {
        "view": "now_ccu",        # どのビュー用か（将来 view02/03 を足す時の識別）
        "source": "steam_official_ccu",  # 出所（公式API観測）。誇張理由は付けない＝観測実数のみ。
        "observed_at": observed_at,       # 代表観測時刻（rows中の最新）。箱の「観測時刻」表示に使う。
        "count": len(rows),
        "rows": rows,
    }

    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")  # 末尾改行＝差分を安定させる

    print(f"書き出し: {OUT_PATH}（{len(rows)} 件・観測時刻 {observed_at}）")
    for r in rows[:5]:
        print(f"  sample appid={r['appid']} {(r['name'] or '')[:30]}: {r['ccu']} 人")
    if not rows:
        # 空でもクラッシュさせない（DBが空/接続直後等）。ワークフローは緑のまま＝無コミット。
        print("注意: 行が0件でした（player_counts がまだ空か、観測直後の可能性）。")


if __name__ == "__main__":
    main()
