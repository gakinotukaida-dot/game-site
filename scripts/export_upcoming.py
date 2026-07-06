"""
表示用エクスポート（候補4「これから来そう」＝近日発売リスト・全ゲーム対象）── 2026-07-06 / v1
================================================================
役割：Neon を「読むだけ」で、**発売前（coming_soon / 発売日が未来）** のゲームを拾い、
      表示の箱(radar_shell)が読む JSON を data/upcoming.json に書き出す。

★重要（分析メモP3の結論）：**跳ね予想（forecast）はしない。** 検証基盤（どの前売りが当たったか）が無い＝過大広告リスク。
  出せるのは「発売前の注目シグナルの検出」まで＝ここでは「近日発売」＋「最近 公式告知があったか（種別のみ）」。
  wishlist 等の前売り指標は公式可否/ToU 未確認のため使わない（グレー源も不採用）。

設計の線：
- DBは読み取り専用（SELECT のみ）。書き込み・スキーマ変更なし＝(B)・非破壊。新規収集ゼロ。
- 「発売前」＝ games.coming_soon=true または release_date が未来。並びは発売日の近い順（未定は後ろ）。
- 「注目」＝ 直近 NEWS_DAYS 日以内に announcements がある（種別のみ・本文/見出しは載せない＝著作物回避）。
- 著作物は載せない。ジャンル等の短い分類語と appid（公式リンクの素）のみ。CCU は無い（発売前＝プレイヤー0）。

戻し方：このファイル/ワークフローを消すだけ（data/upcoming.json が古くなる/消えるのみ・DB無変更）。
env：NEWS_DAYS / GENRE_MAX / LIMIT。
"""

import json
import os
from datetime import datetime

import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/upcoming.json"

NEWS_DAYS = int(os.environ.get("NEWS_DAYS") or "30")   # 「最近の告知」窓（注目シグナル・種別のみ）
GENRE_MAX = int(os.environ.get("GENRE_MAX") or "7")
LIMIT = int(os.environ.get("LIMIT") or "200")          # 近い順に上限

# 発売前ゲーム＋（種別のみの）注目シグナル。跳ね予想はしない。
# has_news = 直近 NEWS_DAYS 日に announcements があるか（本文は取らない）。
QUERY = """
SELECT g.appid, g.name, g.release_date, g.release_date_text, g.genres, g.coming_soon,
       EXISTS(
         SELECT 1 FROM announcements a
         WHERE a.appid = g.appid AND a.published_at >= now() - make_interval(days => %(news_days)s)
       ) AS has_news
FROM games g
WHERE g.coming_soon IS TRUE
   OR (g.release_date IS NOT NULL AND g.release_date > now()::date)
ORDER BY (g.release_date IS NULL), g.release_date ASC NULLS LAST, g.name ASC
LIMIT %(limit)s
"""


def _descs(arr, cap):
    if not isinstance(arr, list):
        return []
    out = []
    for x in arr:
        if isinstance(x, dict):
            d = x.get("description")
            if d and str(d).strip():
                out.append(str(d).strip())
        if len(out) >= cap:
            break
    return out


def _release_iso(rd, text):
    if rd is None:
        return text  # 「近日」「2026 Q3」等のテキスト（あるものだけ・無ければ None）
    try:
        return rd.isoformat()
    except AttributeError:
        return str(rd)


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)  # 物理的に書けないよう固定
        with conn.cursor() as cur:
            cur.execute(QUERY, {"news_days": NEWS_DAYS, "limit": LIMIT})
            recs = cur.fetchall()
    finally:
        conn.close()

    rows = []
    for appid, name, rd, rd_text, genres, coming_soon, has_news in recs:
        rows.append({
            "appid": appid,
            "name": name,
            "release": _release_iso(rd, rd_text),
            "release_known": rd is not None,
            "coming_soon": bool(coming_soon),
            "has_news": bool(has_news),          # 注目シグナル（種別のみ・本文なし）
            "genres": _descs(genres, GENRE_MAX),
        })

    # 注目（has_news）を上に、その中で発売日の近い順（QUERY で概ね近い順・ここで注目を優先）
    rows.sort(key=lambda r: (not r["has_news"], not r["release_known"]))

    payload = {
        "view": "upcoming",
        "source": "games.coming_soon + release_date + announcements(種別のみ)",
        "schema": "upcoming_v1",
        "note": "発売前の注目リスト。跳ね予想はしない（P3結論）。has_news=最近の公式告知の有無のみ。",
        "generated_at": datetime.now().astimezone().isoformat(),
        "params": {"news_days": NEWS_DAYS, "limit": LIMIT},
        "count": len(rows),
        "rows": rows,
    }

    out_dir = os.path.dirname(OUT_PATH)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=False)
        f.write("\n")

    n_news = sum(1 for r in rows if r["has_news"])
    print(f"書き出し: {OUT_PATH}（{len(rows)} 件・発売前／うち最近告知あり {n_news} 件・跳ね予想なし）")
    for r in rows[:8]:
        print(f"  {r['appid']} {(r['name'] or '')[:26]:<26} 発売{r['release'] or '未定'} "
              f"{'[注目]' if r['has_news'] else ''} {'/'.join(r['genres'][:3])}")
    if not rows:
        print("該当0件＝発売前ゲームが games に無い（coming_soon の収集状況次第・正常なこともある）。")


if __name__ == "__main__":
    main()
