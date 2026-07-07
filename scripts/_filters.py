"""
アダルト（成人向け）ゲームを一覧・学習から除外するための共有定義 ── 2026-07-07
================================================================
方針：Steam 自身の判定（content_descriptors）を主軸に、ジャンルも併用して成人向けを判定・除外する。
  - content_descriptors.ids：3=Adult Only Sexual Content / 4=Frequent Nudity or Sexual Content（＝成人向けの決定的シグナル）。
    ※ 1(=Some Nudity)や2(=Violence)や5(=General Mature)は一般作にも付くため“成人向け”には使わない（過剰除外を避ける）。
  - ジャンル "Sexual Content" / "Nudity"（成人向けに特徴的）。
  判定は appdetails 取得時に games.is_adult に保存（＝一度取れば以後は列で高速に除外できる）。

なぜ2層か：is_adult は appdetails でエンリッチ済みの作品にしか付かない（バックフィル待ち）。
  そこで各エクスポートの WHERE では is_adult に加えてジャンル包含（既にジャンルがある作品には即効）も併用する。
env（appdetails 側）：ADULT_DESC_IDS（既定 "3,4"）／ADULT_GENRES（既定 "Sexual Content,Nudity"）。
"""

import os

ADULT_DESC_IDS = {int(x) for x in (os.environ.get("ADULT_DESC_IDS") or "3,4").split(",") if x.strip().isdigit()}
ADULT_GENRES = {g.strip() for g in (os.environ.get("ADULT_GENRES") or "Sexual Content,Nudity").split(",") if g.strip()}


def is_adult_from_details(data):
    """Steam appdetails の data 部から成人向けか判定（True/False）。content_descriptors とジャンルを見る。"""
    if not isinstance(data, dict):
        return False
    cd = data.get("content_descriptors") or {}
    ids = cd.get("ids") or []
    idset = set()
    for x in ids:
        try:
            idset.add(int(x))
        except (TypeError, ValueError):
            pass
    if idset & ADULT_DESC_IDS:
        return True
    for gg in (data.get("genres") or []):
        if isinstance(gg, dict) and (gg.get("description") in ADULT_GENRES):
            return True
    return False


def not_adult(alias="g"):
    """games（別名 alias）から成人向けを除外する WHERE 断片。is_adult（バックフィル後）＋ジャンル包含（即効）の2層。
    NULL 安全：is_adult=NULL や genres=NULL は“成人向けでない”として通す（既知の成人向けだけ落とす）。"""
    a = alias
    g_sex = "'[{\"description\":\"Sexual Content\"}]'::jsonb"
    g_nud = "'[{\"description\":\"Nudity\"}]'::jsonb"
    return ("(" + a + ".is_adult IS NOT TRUE"
            " AND NOT COALESCE(" + a + ".genres @> " + g_sex + ", false)"
            " AND NOT COALESCE(" + a + ".genres @> " + g_nud + ", false))")
