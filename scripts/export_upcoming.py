"""
表示用エクスポート（候補4「これから来そう」＝発売前・羽根予想つき）── 2026-07-07 / v3
================================================================
役割：Neon を「読むだけ」で発売前ゲームを拾い、**羽根予想（跳ね確率）** を付けて data/upcoming.json に書き出す。

★このサイトの核＝羽根予想：複数の実測シグナルを **自社の過去実績で較正したモデル（prelaunch_model.json）** で
  総合し、各作品の「発売直後に跳ねる確率」を出す。当てずっぽうではなく、外れうることは明示（参考にとどめる）。
  使うシグナル（すべて自前観測・ToUクリーン・as-of）：
    体験版CCU / Twitch視聴者 / 配信者数 / 告知数 / 開発元の実績(過去最高CCU・最大レビュー) / ジャンル命中率 / 無料か
  → prelaunch_features.py に定義を集約（学習と推論で同一）。

線：DBは読み取り専用（SELECTのみ）。書き込みは data/upcoming.json 1ファイルのみ・毎回上書き＝可逆。新規収集ゼロ。
    著作物は載せない（ジャンル語・appid・数値のみ）。モデルが無ければ確率は出さず従来の期待度に自動フォールバック。
env：GENRE_MAX / LIMIT / MODEL_PATH。
"""

import json
import os
from datetime import datetime

import psycopg2

import prelaunch_features as F
from _filters import not_adult

DATABASE_URL = os.environ["DATABASE_URL"]
OUT_PATH = os.environ.get("OUT_PATH") or "data/upcoming.json"
MODEL_PATH = os.environ.get("MODEL_PATH") or "data/prelaunch_model.json"

GENRE_MAX = int(os.environ.get("GENRE_MAX") or "7")
LIMIT = int(os.environ.get("LIMIT") or "200")

UPCOMING_WHERE = ("(g.coming_soon IS TRUE OR (g.release_date IS NOT NULL AND g.release_date > now()::date))"
                  " AND " + not_adult("g"))   # ★成人向けは除外

def build_query(web_ok):
    return f"""
WITH {F.cte_prelude()},
self_up AS (
  SELECT g.appid FROM games g WHERE {UPCOMING_WHERE}
),
{F.dev_best_cte('self_up', 'now()')}
SELECT g.appid, g.name, g.release_date, g.release_date_text, g.genres, g.coming_soon,
  {F.feature_sql(asof='now()', web_ok=web_ok)},
  db.dev_best_peak, db.dev_best_reviews
FROM games g
LEFT JOIN dev_best db ON db.appid = g.appid
WHERE {UPCOMING_WHERE}
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
        return text
    try:
        return rd.isoformat()
    except AttributeError:
        return str(rd)


def _load_model():
    try:
        with open(MODEL_PATH, encoding="utf-8") as f:
            m = json.load(f)
        if m.get("base_rate") and m.get("woe"):
            return m
    except (OSError, ValueError):
        pass
    return None


def _iv(v):
    return int(v) if v is not None else None


def compute_rows(conn, limit=None):
    """発売前ゲームを読み、各作品の羽根予想（spike_prob/expect/conf/factors…）を付けた行リストを返す。
    ※ 予測の“単一の源”：これを export（表示）と prediction_log（記録）の両方が使う＝表示と記録の値が必ず一致（skew防止）。
       conn のセッション（readonly 等）は呼び出し側が設定する。並べ替え・payload化は呼び出し側の責務。
    返り値: (rows, model, base, validated)"""
    with conn.cursor() as cur:
        web_ok = F.web_mentions_exists(cur)   # web_mentions が無ければ web_* は NULL（無影響）
        cur.execute(build_query(web_ok), {"limit": (limit if limit is not None else LIMIT)})
        cols = [d[0] for d in cur.description]
        recs = cur.fetchall()

    model = _load_model()
    base = (model.get("base_rate") if model else None) or 0.03
    validated = bool(model and model.get("readiness") == "validated")

    rows = []
    for rec in recs:
        d = dict(zip(cols, rec))
        genres = _descs(d.get("genres"), GENRE_MAX)
        sqlvals = {k: d.get(k) for k in F.SQL_FEATURES}
        news_count = _iv(d.get("news_count")) or 0
        has_news = news_count > 0

        spike_prob = None
        factors = []
        active = 0
        if model:
            s = F.score(model, sqlvals, genres)
            spike_prob = round(s["prob"], 4)
            active = s["active"]
            # 上位の押し上げ要因（跳ねを上げているシグナル）を数個
            factors = [{"name": f["name"], "dir": f["dir"], "bucket": f["bucket"]}
                       for f in s["factors"] if f["dir"] == "up"][:3]

        # 期待度（表示用の粗い3段）＝確率が基準の何倍か。モデルが無ければ実測シグナルの有無で代替。
        if spike_prob is not None and base > 0:
            ratio = spike_prob / base
            expect = "high" if ratio >= 3 else "mid" if ratio >= 1.5 else "low"
        else:
            dc = _iv(d.get("demo_ccu"))
            expect = "high" if (dc and dc >= 50) else "mid" if ((dc and dc > 0) or has_news) else "low"

        # この予測の確からしさ（材料の量×モデルの検証強度）
        if spike_prob is None:
            conf = "na"
        elif validated and active >= 2:
            conf = "high"
        elif active >= 1:
            conf = "mid"
        else:
            conf = "low"

        rows.append({
            "appid": d.get("appid"),
            "name": d.get("name"),
            "release": _release_iso(d.get("release_date"), d.get("release_date_text")),
            "release_known": d.get("release_date") is not None,
            "coming_soon": bool(d.get("coming_soon")),
            "spike_prob": spike_prob,        # ★羽根予想＝跳ね確率（0..1）。モデル無しは null。
            "expect": expect,                # 粗い3段（high/mid/low）
            "conf": conf,                    # 予測の確からしさ（high/mid/low/na）
            "factors": factors,             # 押し上げ要因（name/dir/bucket）
            "active_signals": active,
            # 実測シグナルの生値（表示・監査用）
            "demo_ccu": _iv(d.get("demo_ccu")),
            "twitch_peak": _iv(d.get("twitch_peak")),
            "streamers": _iv(d.get("streamers")),
            "news_count": news_count,
            "has_news": has_news,
            "dev_best_peak": _iv(d.get("dev_best_peak")),
            "dev_best_reviews": _iv(d.get("dev_best_reviews")),
            "web_news": _iv(d.get("web_news")),       # 世界の多言語ニュース記事数（GDELT・最新）
            "web_views": _iv(d.get("web_views")),     # 全言語版Wikipediaの直近ページビュー合計（実閲覧・最新）
            "web_reach": _iv(d.get("web_reach")),     # 言語版Wikipediaの数（Wikidata・最新）
            "is_free": bool(d.get("is_free")),
            "genres": genres,
        })

    return rows, model, base, validated


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        conn.set_session(readonly=True, autocommit=True)
        rows, model, base, validated = compute_rows(conn)
    finally:
        conn.close()

    # ★羽根予想の高い順に並べる（核＝跳ねそうな作品を上に）。モデル無しは expect→発売日で代替。
    _rank = {"high": 0, "mid": 1, "low": 2}
    rows.sort(key=lambda r: (
        -(r["spike_prob"] if r["spike_prob"] is not None else -1),
        _rank.get(r["expect"], 3),
        not r["release_known"],
    ))

    payload = {
        "view": "upcoming",
        "schema": "upcoming_v3",
        "source": "games(coming_soon/release_date) + 実測シグナル(体験版/Twitch/告知/開発元実績/ジャンル) + prelaunch_model",
        "note": ("発売前の羽根予想。spike_prob=跳ね確率＝自社実績で較正したモデルの出力（参考・外れうる）。"
                 "モデルが無い場合は expect のみ（実測シグナルの有無）。"),
        "generated_at": datetime.now().astimezone().isoformat(),
        "model": ({"schema": model.get("schema"), "readiness": model.get("readiness"),
                   "base_rate": model.get("base_rate"), "n_pairs": model.get("n_pairs"),
                   "hit_threshold": (model.get("params") or {}).get("hit_threshold"),
                   "validation_oos": model.get("validation_oos"),
                   "generated_at": model.get("generated_at")} if model else None),
        "params": {"limit": LIMIT, "genre_max": GENRE_MAX},
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
    print(f"書き出し: {OUT_PATH}（{len(rows)} 件・発売前）"
          f" model={'あり('+str(model.get('readiness'))+')' if model else 'なし'}"
          f" 期待度高 {n_high} / 最近告知 {n_news}")
    for r in rows[:10]:
        p = f"{r['spike_prob']*100:.1f}%" if r["spike_prob"] is not None else "—"
        drv = ",".join(f["name"] for f in r["factors"])
        print(f"  跳ね {p:<6} [{r['expect']:<4}/{r['conf']:<4}] {str(r['appid']):<8} "
              f"{(r['name'] or '')[:22]:<22} demo={r['demo_ccu']} tw={r['twitch_peak']} "
              f"devbest={r['dev_best_peak']} 要因={drv}")
    if not rows:
        print("該当0件＝発売前ゲームが games に無い（正常なこともある）。")


if __name__ == "__main__":
    main()
