"""
表示用エクスポート（候補4「これから来そう」＝近日発売リスト・全ゲーム対象）── 2026-07-06 / v1
================================================================
役割：Neon を「読むだけ」で、**発売前（coming_soon / 発売日が未来）** のゲームを拾い、
      表示の箱(radar_shell)が読む JSON を data/upcoming.json に書き出す。

★重要（分析メモP3の結論）：**跳ねを断定する「予測（forecast）」はしない。** 検証基盤（どの前売りが当たったか）が無い＝過大広告リスク。
  代わりに出せるのは「発売前の実測シグナル」から作る **期待度（参考）** まで＝断定ではなく参考値。
  材料は (1)「近日発売」(2)「最近 公式告知があったか（種別のみ）」(3)「体験版が実際に遊ばれているか＝体験版CCU（実測）」。
  wishlist 等の前売り指標は公式可否/ToU 未確認のため使わない（グレー源も不採用）。体験版CCU は自前の player_counts 実測でクリーン。
  期待度 expect = high(体験版CCU≥しきい値) / mid(体験版に人 or 最近告知) / low(観測シグナルなし)。★あくまで参考・断定しない。
  検証済み「予測」への格上げは、発売前→発売後の対を裏で貯める別バッチ（prelaunch_backtest）で当たりが確認できてから。

設計の線：
- DBは読み取り専用（SELECT のみ）。書き込み・スキーマ変更なし＝(B)・非破壊。新規収集ゼロ。
- 「発売前」＝ games.coming_soon=true または release_date が未来。並びは発売日の近い順（未定は後ろ）。
- 「注目」＝ 直近 NEWS_DAYS 日以内に announcements がある（種別のみ・本文/見出しは載せない＝著作物回避）。
- 著作物は載せない。ジャンル等の短い分類語と appid（公式リンクの素）のみ。CCU は無い（発売前＝プレイヤー0）。

戻し方：このファイル/ワークフローを消すだけ（data/upcoming.json が古くなる/消えるのみ・DB無変更）。
env：NEWS_DAYS / DEMO_DAYS / EXPECT_DEMO_HIGH / GENRE_MAX / LIMIT。
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
DEMO_DAYS = int(os.environ.get("DEMO_DAYS") or "14")   # 体験版CCUを見る直近日数
EXPECT_DEMO_HIGH = int(os.environ.get("EXPECT_DEMO_HIGH") or "50")   # 「期待度 高」の体験版同時接続しきい値（暫定）

# 発売前ゲーム＋実測できる注目シグナル。★跳ねを断定する"予測"はしない（P3）。出せるのは「期待度（参考）」まで。
# has_news  = 直近 NEWS_DAYS 日に announcements があるか（本文は取らない）。
# demo_ccu  = この作品の体験版（fullgame_appid=自分）の直近 DEMO_DAYS 日の最大同時接続
#             ＝「体験版が実際に遊ばれている」＝発売前の実測モメンタム（ToUクリーン・グレー源不使用）。
QUERY = """
SELECT g.appid, g.name, g.release_date, g.release_date_text, g.genres, g.coming_soon,
       EXISTS(
         SELECT 1 FROM announcements a
         WHERE a.appid = g.appid AND a.published_at >= now() - make_interval(days => %(news_days)s)
       ) AS has_news,
       (SELECT max(pc.player_count)
          FROM player_counts pc
          JOIN games dg ON dg.appid = pc.appid AND dg.fullgame_appid = g.appid
          WHERE pc.recorded_at >= now() - make_interval(days => %(demo_days)s)) AS demo_ccu
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
            cur.execute(QUERY, {"news_days": NEWS_DAYS, "demo_days": DEMO_DAYS, "limit": LIMIT})
            recs = cur.fetchall()
    finally:
        conn.close()

    rows = []
    for appid, name, rd, rd_text, genres, coming_soon, has_news, demo_ccu in recs:
        dc = int(demo_ccu) if demo_ccu is not None else None
        # 期待度（参考）＝実測シグナルのみ。★跳ねの断定はしない（P3）。強い順に high / mid / low。
        #   high : 体験版が実際に遊ばれている（demo_ccu >= しきい値）＝最も硬い前売りシグナル
        #   mid  : 体験版に人がいる or 最近の公式告知あり（弱いが実測）
        #   low  : いまは観測シグナルなし（発売前としては普通・まだ材料が無いだけ）
        if dc is not None and dc >= EXPECT_DEMO_HIGH:
            expect = "high"
        elif (dc is not None and dc > 0) or has_news:
            expect = "mid"
        else:
            expect = "low"
        rows.append({
            "appid": appid,
            "name": name,
            "release": _release_iso(rd, rd_text),
            "release_known": rd is not None,
            "coming_soon": bool(coming_soon),
            "has_news": bool(has_news),          # 注目シグナル（種別のみ・本文なし）
            "demo_ccu": dc,                      # 体験版の直近最大同時接続（実測・無ければ None）
            "expect": expect,                    # 期待度（参考）＝high/mid/low。断定的な跳ね予想ではない。
            "genres": _descs(genres, GENRE_MAX),
        })

    # 期待度の高い順→体験版CCU→注目→発売日既知 の順に並べる（材料の硬いものを上に）。
    _rank = {"high": 0, "mid": 1, "low": 2}
    rows.sort(key=lambda r: (
        _rank.get(r["expect"], 3),
        -(r["demo_ccu"] or 0),
        not r["has_news"],
        not r["release_known"],
    ))

    payload = {
        "view": "upcoming",
        "source": "games.coming_soon + release_date + announcements(種別のみ) + 体験版CCU(実測)",
        "schema": "upcoming_v2",
        "note": "発売前の注目リスト。跳ねの断定的予測はしない（P3）。expect=期待度（参考）＝実測シグナル（体験版CCU/最近の公式告知）のみで算出。",
        "generated_at": datetime.now().astimezone().isoformat(),
        "params": {"news_days": NEWS_DAYS, "demo_days": DEMO_DAYS, "expect_demo_high": EXPECT_DEMO_HIGH, "limit": LIMIT},
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
    n_high = sum(1 for r in rows if r["expect"] == "high")
    n_mid = sum(1 for r in rows if r["expect"] == "mid")
    print(f"書き出し: {OUT_PATH}（{len(rows)} 件・発売前／うち最近告知あり {n_news} 件・"
          f"期待度 高 {n_high} / 中 {n_mid} 件・跳ねの断定予測なし）")
    for r in rows[:8]:
        dc = r["demo_ccu"]
        print(f"  [{r['expect']:<4}] {r['appid']} {(r['name'] or '')[:24]:<24} 発売{r['release'] or '未定'} "
              f"{('体験版CCU=' + str(dc)) if dc else ''} {'[注目]' if r['has_news'] else ''} {'/'.join(r['genres'][:3])}")
    if not rows:
        print("該当0件＝発売前ゲームが games に無い（coming_soon の収集状況次第・正常なこともある）。")


if __name__ == "__main__":
    main()
