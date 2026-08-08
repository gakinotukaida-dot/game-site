"""
詳細ページ「情報」節の材料（detail）を組み立てる共有部品 ── 2026-08-09
================================================================
役割：`export_now_ccu.py` と `export_upcoming.py` の両方が、**同じキー・同じ形**の detail を出すための1か所。
  片方だけ形が違うと表示側（index.html の `detailBodyReal`）に分岐が増える。
  `appdetails_sweep.py` の複製が残タスクとして残っている前例があるので、ここは最初から共有にする。

線：DBは読み取り専用（SELECT のみ）。新規収集ゼロ＝既に `games` / `player_counts` にある値だけで作る。
    著作物は載せない（開発元名・分類語・appid・数値のみ）。

★`with_stats` について（発売前の作品で最も大事な一点）
  発売前の作品には CCU の観測が存在しない。だからといって `stats: {}` や `history: []` を**空の器で渡してはいけない**。
  index.html の `hasHist` は `o.detail.stats` の**存在**を見るので、空の辞書でも真になり
  「観測の履歴：準備中」が復活する。空の器は「無い」ではなく「あるが未取得」と読まれる（`0802E-05`）。
  → 発売前は `with_stats=False` で**キーごと出さない**。
"""

from _filters import not_adult

# 同じ開発元の他作品（自分を除く・最新CCU降順）。読み取りのみ。
SIBLING_QUERY = """
SELECT g2.appid, g2.name,
       (SELECT pc.player_count FROM player_counts pc
        WHERE pc.appid = g2.appid ORDER BY pc.recorded_at DESC LIMIT 1) AS ccu
FROM games g2
WHERE g2.developers ?| %s AND g2.appid <> %s
  AND """ + not_adult("g2") + """
ORDER BY ccu DESC NULLS LAST
LIMIT %s
"""


def names(arr, cap):
    """文字列配列（developers / publishers）→ 空白を落とした先頭 cap 件。"""
    if not isinstance(arr, list):
        return []
    out = [str(x).strip() for x in arr if x and str(x).strip()]
    return out[:cap]


def descs(arr, cap):
    """[{id,description},...]（genres / categories）→ 説明文の配列（先頭 cap 件）。"""
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


def fetch_siblings(cur, developers, self_appid, limit):
    """同じ開発元の他作品を取る（読み取りのみ）。開発元名が無ければ**照会しない**（空を返すだけ）。
    発売前の作品ではここが最も価値がある＝「この開発元は前に何を作ったか」はストアページから辿りにくい。"""
    devs = names(developers, 50)
    if not devs:
        return []
    cur.execute(SIBLING_QUERY, (devs, self_appid, limit))
    return [{"appid": r[0], "name": r[1], "ccu": r[2]} for r in cur.fetchall()]


def build_detail(it, dev_max, genre_max, category_max, with_stats=True, dlc_known=True):
    """詳細ページの「情報」節が読む detail を組み立てる。
    it に期待するキー：developers / publishers / genres / categories / dlc / release_date /
      release_date_text / website / _siblings / _market（/ with_stats のとき _stats・_history）。
    ★release_date は JSON に載せられる形（文字列）で渡すこと。date 型のままだと json.dump で落ちる。
    dlc_known：DLC の有無を**調べたことがある**か。False なら dlc_count を None にする。
      `games.dlc` は「DLCが無い」ときも「appdetails をまだ引いていない」ときも NULL なので、
      未到達の作品で 0 を渡すと表示側が「DLC なし」と**断定**してしまう（`0802E-05` と同型の嘘）。
      発売後の作品（now_ccu）は appdetails 到達済みが前提なので既定 True＝従来と同じ 0/正の整数。"""
    dlc = it.get("dlc")
    dlc_count = len(dlc) if isinstance(dlc, list) else 0
    d = {
        "developers": names(it.get("developers"), dev_max),
        "publishers": names(it.get("publishers"), dev_max),
        "genres": descs(it.get("genres"), genre_max),
        "categories": descs(it.get("categories"), category_max),
        "dlc_count": dlc_count if dlc_known else None,
        "release": it.get("release_date") or it.get("release_date_text"),
        "website": it.get("website"),
        "siblings": it.get("_siblings") or [],
    }
    if with_stats:
        # v5：観測の履歴（無い/浅い場合は None/空＝箱が「履歴が浅い」を出す）
        st = it.get("_stats") or {}
        d["stats"] = {
            "baseline": st.get("baseline"),
            "peak24h": st.get("peak24h"),
            "peak_observed": st.get("peak_observed"),
            "n_points": st.get("n_points", 0),
        }
        d["history"] = it.get("_history") or []
    # ⑨-a：価格・レビュー。未取得は None（0円・好評率0% と読めてはいけない＝0802E-05）。
    mk = it.get("_market") or {}
    d["price"] = mk.get("price")
    d["review"] = mk.get("review")
    return d


def has_body(d):
    """表示側 `detailBodyReal` の `hasAny` と同じ判定＝**「情報」節が実際に描画されるか**。
    ここを表示側と揃えておかないと、ログの件数が画面と食い違う（報告ではなく現物で測るための計器）。"""
    if not d:
        return False
    return bool(d.get("developers") or d.get("publishers") or d.get("categories")
                or d.get("release") or (d.get("dlc_count") or 0) > 0
                or d.get("siblings") or d.get("website") or d.get("price") or d.get("review"))


def has_appdetails(d):
    """appdetails 由来の中身が1つでもあるか（`release` を**除く**）。
    `release` は発売前の行なら元から入っているので、これを数に入れると
    「開発元等は既にDBにある」という推定が当たったかを測れない（母数200のうちほぼ全件が真になる）。"""
    if not d:
        return False
    return bool(d.get("developers") or d.get("publishers") or d.get("categories")
                or (d.get("dlc_count") or 0) > 0 or d.get("siblings") or d.get("website"))
