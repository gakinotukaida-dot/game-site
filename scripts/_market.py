"""
価格・割引・レビュー好評率を「各appidの最新値」で引く共有ヘルパー ── ⑨-a（2026-08-08）
=====================================================================================
役割：既に収集済みの値を、各エクスポートの detail に載せる形へ整えるだけ。
  - 価格・割引 … appdetails_sweep.py が price_snapshots へ入れている
    （currency / initial / final / discount_percent。cc=jp なので通貨は JPY、
     金額は Steam の price_overview そのままで **最小単位（×100）**＝¥2,980 は 298000）。
  - レビュー   … review_sweep.py が review_snapshots へ入れている
    （total_positive / total_negative / total_reviews / review_score）。

守る線
  - **DBは読み取り専用（SELECT のみ）。** 書き込み・スキーマ変更は一切しない＝収集側は無改変。
  - **取れなかったものは None を入れる（キーを消さない）。** 「0円」「好評率0%」と読めてはいけない
    （0802E-05：0件と読めなかったを区別する）。呼び出し側は None を「—」で出すこと。
  - **片方のテーブルが無い／壊れていても本体の出力を止めない。** 例外は握って空の辞書を返す
    （エクスポートが落ちると公開サイトは古いJSONのまま静かに死ぬ＝0802D の型）。
  - 金額の単位換算はここではしない。生の値と通貨コードをそのまま渡し、表示側で整える
    （通貨が JPY 以外に変わっても壊れないようにするため）。
"""

PRICE_SQL = """
SELECT DISTINCT ON (appid) appid, currency, initial, final, discount_percent
FROM price_snapshots
WHERE appid = ANY(%s)
ORDER BY appid, recorded_at DESC
"""

REVIEW_SQL = """
SELECT DISTINCT ON (appid) appid, total_positive, total_reviews
FROM review_snapshots
WHERE appid = ANY(%s)
ORDER BY appid, recorded_at DESC
"""


def _int_or_none(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def fetch_market(cur, appids):
    """{appid: {"price": {...}|None, "review": {...}|None}} を返す。appids が空なら {}。

    price  = {"currency", "initial", "final", "discount_percent"}   ← 金額は最小単位のまま
    review = {"positive", "total", "pct"}                           ← pct は好評率(0-100, 小数1桁)
    どちらも「そのテーブルに1件も無い作品」では None（＝未取得。0ではない）。
    """
    ids = [a for a in (appids or []) if a is not None]
    out = {}
    if not ids:
        return out
    try:
        cur.execute(PRICE_SQL, (ids,))
        for a, cur_code, initial, final, disc in cur.fetchall():
            out.setdefault(a, {})["price"] = {
                "currency": cur_code,
                "initial": _int_or_none(initial),
                "final": _int_or_none(final),
                "discount_percent": _int_or_none(disc),
            }
    except Exception as e:                      # noqa: BLE001 ── 出力は止めない（価格が無いだけ）
        print(f"  ⚠ market[price] skip: {type(e).__name__}: {e}")
    try:
        cur.execute(REVIEW_SQL, (ids,))
        for a, pos, total in cur.fetchall():
            p, t = _int_or_none(pos), _int_or_none(total)
            # 総数0/未取得のときは pct を出さない（0% と読めてはいけない）。
            pct = round(p * 100.0 / t, 1) if (p is not None and t) else None
            out.setdefault(a, {})["review"] = {"positive": p, "total": t, "pct": pct}
    except Exception as e:                      # noqa: BLE001
        print(f"  ⚠ market[review] skip: {type(e).__name__}: {e}")
    return out


def market_of(mkt, appid):
    """detail へ載せる2キーを常に同じ形で返す（未取得は None＝キーは消さない）。"""
    m = (mkt or {}).get(appid) or {}
    return {"price": m.get("price"), "review": m.get("review")}
