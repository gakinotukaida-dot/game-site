#!/usr/bin/env python3
# トレパ / Trend Pulse — 伸びフィード生成（読み取り専用：data/view02_rising.json のみ参照・DB非依存・鍵不要）
# 出力: data/rising_feed.xml（RSS 2.0・どのRSSリーダーでも購読可）
# 任意: 環境変数 DISCORD_WEBHOOK が設定されていれば、前回フィードに無い「新規の伸び作品」だけを Discord に通知（重複防止）。
# 実行: ローカル `python scripts/make_feed.py` / CI は .github/workflows/make_feed.yml（schedule + 手動）。
import json, os, sys, html, re
from datetime import datetime, timezone
from email.utils import format_datetime
import urllib.request

SITE_URL  = os.environ.get("SITE_URL", "https://gakinotukaida-dot.github.io/game-site/")
SRC       = "data/view02_rising.json"
OUT       = "data/rising_feed.xml"
MIN_RATIO = float(os.environ.get("FEED_MIN_RATIO", "1.3"))
MAX_ITEMS = int(os.environ.get("FEED_MAX_ITEMS", "25"))

CAUSE_JA = {"sale": "セール", "news": "更新/告知", "launch": "新作", "free_promo": "無料配布",
            "review_surge": "レビュー増", "b1_discovery": "配信で発掘", "b1_attention": "配信で注目",
            "jp_news": "国内で話題", "web_buzz": "Web/ニュースで話題"}
CONF_JA  = {"high": "確度 高", "mid": "確度 中", "low": "確度 低"}


def parse_iso(s):
    try:
        return datetime.fromisoformat((s or "").replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)


def causes_text(signals):
    out = []
    for s in signals or []:
        t = s.get("type"); lab = CAUSE_JA.get(t, t); v = s.get("value") or {}
        if t == "sale" and v.get("discount_percent") is not None:
            lab += f" {v['discount_percent']}%OFF"
        elif t == "review_surge" and v.get("delta") is not None:
            lab += f" +{v['delta']}"
        elif t == "web_buzz" and v.get("articles") is not None:
            lab += f"（記事{v['articles']}）"
        out.append(lab)
    return "・".join(out) if out else "きっかけは調査中"


def load_old_guids(path):
    try:
        return set(re.findall(r"<guid[^>]*>([^<]+)</guid>", open(path, encoding="utf-8").read()))
    except Exception:
        return set()


def main():
    try:
        d = json.load(open(SRC, encoding="utf-8"))
    except Exception as e:
        print(f"[feed] {SRC} を読めません（スキップ）: {e}", file=sys.stderr)
        return 0

    meta = d.get("meta") or {}
    gen = parse_iso(meta.get("generated_at", ""))
    gen_date = gen.date().isoformat()
    pub = format_datetime(gen)

    items = [it for it in (d.get("items") or [])
             if (it.get("detection") or {}).get("ratio", 0) >= MIN_RATIO][:MAX_ITEMS]

    old_guids = load_old_guids(OUT)
    entries, fresh = [], []
    for it in items:
        appid = it.get("appid"); name = it.get("name", "(不明)")
        det = it.get("detection") or {}
        ratio = det.get("ratio") or 1; now = det.get("current_ccu") or 0; base = det.get("baseline") or 0
        conf = CONF_JA.get(it.get("confidence"), "")
        cz = causes_text(it.get("signals"))
        guid = f"{appid}-{gen_date}"                 # 1作品につき1日1エントリ（RSSリーダーの多重通知を防ぐ）
        title = f"{name} が ふだんの ×{ratio:.1f}"
        desc = f"今 {now:,} ／ 平常 {base:,}（過去14日の中央値）・きっかけ：{cz}・{conf}"
        link = f"https://store.steampowered.com/app/{appid}/"
        entries.append((guid, title, desc, link))
        if guid not in old_guids:
            fresh.append((name, ratio, cz, link))

    def esc(s):
        return html.escape(str(s), quote=True)

    parts = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">', '<channel>',
             '<title>トレパ / Trend Pulse — いつもより伸びているPCゲーム</title>',
             f'<link>{esc(SITE_URL)}</link>',
             f'<atom:link href="{esc(SITE_URL)}data/rising_feed.xml" rel="self" type="application/rss+xml" />',
             '<description>平常（過去14日の中央値）より人が増えているPCゲームの観測。非公式・実験的な指標です。</description>',
             '<language>ja</language>', f'<lastBuildDate>{pub}</lastBuildDate>']
    for guid, title, desc, link in entries:
        parts += ['<item>', f'<title>{esc(title)}</title>', f'<link>{esc(link)}</link>',
                  f'<guid isPermaLink="false">{esc(guid)}</guid>', f'<pubDate>{pub}</pubDate>',
                  f'<description>{esc(desc)}</description>', '</item>']
    parts += ['</channel>', '</rss>', '']

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    open(OUT, "w", encoding="utf-8").write("\n".join(parts))
    print(f"[feed] {OUT} に {len(entries)} 件（うち新規 {len(fresh)} 件）を書き出しました。")

    hook = os.environ.get("DISCORD_WEBHOOK", "").strip()
    if hook and fresh:
        lines = [f"**いつもより伸びているPCゲーム**（{gen_date}）"]
        for name, ratio, cz, link in fresh[:5]:
            lines.append(f"・**{name}** ふだんの ×{ratio:.1f}（{cz}） <{link}>")
        if len(fresh) > 5:
            lines.append(f"…ほか {len(fresh) - 5} 件")
        payload = json.dumps({"content": "\n".join(lines)}).encode("utf-8")
        try:
            req = urllib.request.Request(hook, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=15)
            print(f"[feed] Discord に新規 {min(len(fresh), 5)} 件を通知しました。")
        except Exception as e:
            print(f"[feed] Discord 通知に失敗（無視して続行）: {e}", file=sys.stderr)
    elif hook:
        print("[feed] Discord: 新規の伸び作品なし（通知なし）。")
    else:
        print("[feed] Discord: DISCORD_WEBHOOK 未設定のためスキップ（RSSは生成済み）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
