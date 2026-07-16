# 「調査中（原因不明）」の調査アルゴリズム — 設計・決定記録

対象: `scripts/view02_rank_v2.py`（本番ランキング）→ `data/view02_rising.json` → `scripts/make_feed.py`（RSS）
状態: **方針確定・Phase1 実装済み・Phase2 A 実装済み（既定OFF）**。本書は経緯・判定・決定・懸念消し込みの決定記録であり、実装仕様も兼ねる。
- Phase1（B 透明性 + D プロセス指標）：`view02_rank_v2.py` の `investigation` ブロック（env `INV_META`）、
  `scripts/diagnose_cause_accuracy.py` ＋ `.github/workflows/cause_scorecard.yml`（`data/cause_scorecard.json`）。
- Phase2 A（主因/併発のタイミング整合）：`view02_rank_v2.py` の `apply_timing_alignment()`（env `TIMING_ALIGN`・**既定OFF**）。
  ソフト（除外しない・signals を消さない）。残るは D-リフト（履歴蓄積後）と C（休眠）。

---

## 0. 用語

- **きっかけ / signal / trigger**: 急上昇の原因候補（セール・告知・新作・無料配布・レビュー急増・国内話題・Web話題・配信）。
- **調査中 / 原因不明**: シグナルが1つも立たなかった作品。サイト／RSSで「調査中」「原因不明 / Cause unknown」と表示。
- **主因(aligned) / 併発(context)**: 伸びの立ち上がりと時刻が整合する原因を主因、同時に存在するだけのものを併発と呼ぶ（Phase2 で導入）。

## 1. 背景と当初の狙い

急上昇作品はシグナル0で「調査中」と表示される。未知の原因を掘り起こせる唯一の手段が **GDELT（世界の多言語ニュース記事数）** だが、従来は **倍率上位 `WEB_CAND_N=20` 件固定**で照会していたため、

- 倍率上位でも既にセール等で判明済みの作品にGDELT枠を消費し、
- 倍率が下位で他シグナルの無い＝**まさに「調査中」の作品**はWeb調査を一切受けられなかった。

当初の狙いは「**現在『調査中』の項目をちゃんと調査するようアルゴリズムに組み込む**」こと。

## 2. 経緯

| 段階 | 結論 |
|---|---|
| PR#39（実装済み） | GDELT枠を **調査中優先・全件保証**（`WEB_UNKNOWN_ALL`・上限 `WEB_MAX_QUERIES`）に変更。`make_feed.py` の `web_buzz` ラベル欠落も修正。テスト済み・env可逆 |
| 4方針の提示 | A 因果精度 / B 透明性 / C 深掘り / D 評価 |
| 再評価で判明した事実 | 下表（§3） |
| 最終決定 | §5。**B → D(プロセス指標) を先に固め、A は精度層として後、C は休眠** |

## 3. 確定した事実（コードで裏取り済み）

| # | 事実 | 根拠 |
|---|---|---|
| F1 | ランキング本体は **DB read-only 固定**（SELECTのみ・書き込み不可） | `view02_rank_v2.py` `conn.set_session(readonly=True)` |
| F2 | **Twitch数値は公開しない**設計が明文化（公開JSONは種別のみ `value=None`） | 同ファイル「公開JSONは種別のみ・数値なし（②）」「数値あり…公開しない」 |
| F3 | `web_buzz` の記事数は既に公開済み（`value.articles`） | 同ファイル `web_buzz` signal / `index.html` が記事数を表示 |
| F4 | `news` は `days_ago`（相対時刻）で保持、`jp_news` は突合に時刻を持たない | `cause_sets`：news=days_ago / jp_news は `SELECT title` のみ |
| F5 | `web_mentions` テーブルは **発売前ゲーム専用**（`WHERE coming_soon IS TRUE`）。発売済み急上昇作品のデータは無い | `web_mentions_sweep.py` `TARGET_QUERY` |
| F6 | `wikipedia_pageviews` は既定スイープ対象外（重い）。閲覧サージは未収集 | `web_mentions_sweep.py` `_DEFAULT_SOURCES` |
| F7 | `view02_rising.json` は **変更時のみコミット**（＝git履歴で as-of 復元が可能） | `view02_rank_v2.yml` 「変更があるときだけ」 |
| F8 | 既存の消費側（make_feed 等）は `.get` 参照 → **加算フィールドで壊れない** | `make_feed.py` `s.get(...)` / `it.get(...)` |

**最重要の含意**: PR#39 で*カバレッジ*は改善済み。GDELT の先に残る「原因」ソースはすべて注目の代理指標＝**循環**（F5/F6 で重い・未収集）。よって**新規カバレッジ(C)は収穫逓減**で、残る高価値は「**調査中を正直に説明する(B)**」と「**それを測る(D)**」に移る。

## 4. 不変条件（全フェーズで守る）

1. **DB read-only**（F1）。既存テーブルへの書き込みを増やさない。
2. **公開禁止データを載せない**（F2）。Twitch数値はリポジトリ内（JSON・ログ・doc）に一切残さない＝真偽のみ。
3. **signals を消さない**。再重み付け・ラベル付与はしても、存在するシグナルを削って「調査中」を増やさない。
4. **env 可逆**。新挙動はフラグで従来へ戻せる。既定は保守側（誤ラベルより無ラベル）。

## 5. 各方針の最終判定

| 方針 | 判定 | 根拠 |
|---|---|---|
| **B 透明性** | **採用・最優先** | 当初目的（調査中の正直さ）に直効。read-only。Dの前提 |
| **D 評価** | **採用・Bと同時（プロセス指標）／リフトは後** | 調査中が減っているか測る唯一の物差し |
| **A 精度** | **採用・後続（精度層）** | 誤主因を減らす。ただしカバレッジには効かないと正直化 |
| **C 深掘り** | **延期・休眠** | 前提崩壊（F5 発売前専用）＋循環＋重い（F6）。write必要で不変条件1も破る |

## 6. 残存懸念の消し込み

| # | 懸念 | 消し込み |
|---|---|---|
| PR39-1 | GDELT呼が20→最大45で実行+~25s・429増 | 失敗は作品単位skipで安全／締切なしで許容／`WEB_MAX_QUERIES`上限・env調整可 |
| B-1 | Twitch数値保存＝公開設計違反(F2) | **真偽のみ保存・数値は載せない**（articlesは既公開F3でOK） |
| B-2 | `investigation` が冗長 | 陰性/未照会だけが新価値→`queried/hit`真偽＋既公開数値にスリム化 |
| B-3 | 追記ログでリポジトリ肥大・rebase衝突 | **新ファイル不要**：JSONに埋め、Dは `view02_rising.json` の **git履歴で as-of 復元**（F7）→肥大ゼロ |
| A-1 | 調査中に効かない | 「精度層」に正し優先度↓・正直化 |
| A-2 | オンセット推定が粗い | **ソフト降格のみ・ハード除外なし** |
| A-3 | 時刻無シグナル多くunknown多発 | `aligned:null=中立`（フルboost）／MVPは新クエリ不要（news=days_ago F4・launch=release_date） |
| A-4 | Aが調査中を増やす副作用 | **signalsは除去せず再重み付けのみ→known維持・調査中は増えない**（不変条件3） |
| C-1 | web_mentionsが発売前専用(F5) | 「SELECTだけ」不可・延期 |
| C-2 | 閲覧サージ未収集・重い(F6) | rank本体(read-only・4x日)に重API載せない |
| C-3 | 注目サージは循環 | trigger禁止・**併発(context)のみ**・休眠 |
| D-1 | lead率がAと循環 | 独立指標から除外 |
| D-2 | 原因別hit率≠帰属正しさ（過大主張） | 「**既知 vs 調査中の前方持続リフト**」に再定義・sanity checkと明示 |
| D-3 | リフトに選択バイアス(launch/free) | 相関≠因果と明示・**プロセス指標を主**、リフトは監視補助 |
| X-1 | 新フィールドで消費側破損 | `.get`参照で加算無害（F8・PR#39と同じ論拠） |
| X-2 | DB書き込みでread-only破り | B/D/Aは全read-only。writeが要るCのみ→**Cを延期＝不変条件を守る** |

**未消化の懸念：なし。**

## 7. フェーズ計画

```
PR#39（済）= カバレッジ。維持（テスト済・可逆）
 └─ Phase1【実装済み】: B（透明性・修正版）+ D（プロセス指標）
 └─ Phase2:
      A【実装済み・既定OFF】: 主因/併発のタイミング整合（ソフト・env TIMING_ALIGN）
      D-リフト【未実装】: 既知 vs 調査中の前方持続差（履歴が貯まってから）
 └─ C: 休眠（B/Dで真の未知が支配的、かつ非循環の新ソースが出た場合のみ再検討）
```

### Phase2 A の運用（有効化の順序・決定済み 2026-07-16）

A は既定 OFF。有効化前の手順（review_surge relative と同じ「休眠導入→分布確認→有効化」の定石）:

1. **マージ後、`diagnose_timing_align.yml` を数回（別日に分けて）手動実行**し、lead サンプルを**10件程度**貯める。
   1回の実行で得られる時刻付きサンプルは news/launch の 2〜3 件（実測）＝ 4〜5 回の実行が目安。
2. **判断基準**: news の lead が 3 日超の事例は「古い告知＋別の強いきっかけ（セール等）が併存」なら**正しい降格**
   （窓は緩めない）。**newsが真の主因なのに窓外**（他きっかけ無し・告知直後から漸増）という事例が複数出た場合のみ
   `CAUSE_LOOKBACK_DAYS` を 5 へ緩和検討。launch は lead +2.0日×2 が整合済みで現行窓で問題なし。
3. しきい値確定後、本番 `view02_rank_v2.yml` に `TIMING_ALIGN: "1"` を追加して点灯（env のみ・可逆）。

CI実測（2026-07-16・2回）: 主因の時刻確認 1/15・1/18。t_rise 推定 30/30。
news lead=+4.9/+4.9/+5.9（3件とも非整合＝古い告知とセール併存の降格で妥当と判断）、launch lead=+2.0×2（整合）/+10.5（非整合・発売から日が経った作品で妥当）。
有効化しても signals は消えず `known` も不変（＝調査中を増やさない）。非整合は併発として boost を弱めるだけで、順位への影響は上限付き。

### Phase1 仕様（実装対象）

**B: `view02_rising.json` の各 item に調査メタを追加（公開安全・集計のみ）**

```jsonc
"investigation": {
  "queried": ["sale","news","launch","free","review","jp_news","twitch","web"],
  "results": {
    "web":    {"queried": true,  "hit": true,  "articles": 12},   // 記事数は既公開(F3)なのでOK
    "twitch": {"queried": true,  "hit": false},                   // 数値は載せない(F2)
    "sale":   {"hit": false}, "news": {"hit": false}, ...         // 陰性も記録＝新価値
  },
  "unknown_reason": null
  //  "investigated_all_negative" = 全ソース調べて陰性（＝正直な調査中）
  //  "web_skipped_budget"        = GDELT予算切れで未照会（カバレッジ欠落・PR#39後は稀）
  //  "web_query_failed"          = GDELT照会が失敗（レート制限等・再試行済み）＝「調べ尽くした」とは言えない
  //  "twitch_key_absent"         = 鍵なしでB1未実施
}
```

CI実測（2026-07-16 初回・TIMING_ALIGN=1 診断実行）で GDELT が 30 件中 21 件 HTTPError（レート制限）と判明。
対策として web_fetch に GDELT_RETRY_WAIT（既定10s）での1回再試行を追加し、失敗作品は `web.error=true` +
`unknown_reason="web_query_failed"` で「陰性」と区別する（失敗を『調べ尽くした』と偽らない＝Bの核）。

- `unknown_reason` は **調査中 item のみ**に意味を持たせる（既知 item は `null`）。
- 追記ログ用の新ファイルは作らない（B-3）。永続化は F7 の git 履歴に委ねる。
- env: `INV_META`（既定 ON）。OFF で従来JSONに戻る。

**D: `scripts/diagnose_cause_accuracy.py`（read-only・ログ/JSON出力・コミット任意）**

プロセス指標（Bから即算出・confound無し）:
- `unknown_rate` = 調査中 / 全 item
- 調査中の内訳: `investigated_all_negative` / `web_skipped_budget` / `twitch_key_absent` の比
- `web_resolve_rate` = PR#39 の `n_web_hit / len(unresolved)`

出力 `data/cause_scorecard.json`（他 `diagnose_*` と同形式）。ワークフローは `workflow_dispatch`（手動）。

### Phase2 仕様（後続）

**A: 主因(aligned)/併発(context) の二層化（ソフト・env既定OFF）**

- `t_rise` = `history`（6hバケット）で `baseline + (peak−baseline)×RISE_ONSET_FRAC` を初めて超えたバケット時刻。
- MVP は **news / launch のみ**整合判定（F4 により news=days_ago・launch=release_date で**新クエリ不要**）。他は `aligned:null`＝中立。
- 整合したものは主因（フルboost）、非整合は併発（boost ×`CAUSE_MISALIGN_MULT`、既定0.4）。**除去しない**（不変条件3・A-4）。
- 出力: 各 signal に `onset_ts`/`aligned`、item に `primary_cause`。
- env: `TIMING_ALIGN`（既定 OFF）、`CAUSE_LOOKBACK_DAYS`=3、`CAUSE_LAG_DAYS`=1、`RISE_ONSET_FRAC`=0.5、`CAUSE_MISALIGN_MULT`=0.4。

**D-リフト（履歴蓄積後）**: 既知原因 item と調査中 item の **前方持続率の差**（`backtest_hit` と同じ as-of 手法・F7 のgit履歴で as-of 原因ラベルを復元）。相関であり帰属の真偽ではない旨を明記（D-2/D-3）。

### 延期（C）

`wiki_surge` 等の深掘りは、(i) 発売済み作品への Web 収集拡張（`web_mentions` スイープの対象拡大 or 新スイープ）と (ii) 別オフラインスイープ→テーブル→cheap read の分離が前提。rank 本体には載せない。**併発(context)** としてのみ扱い、trigger にはしない。B/D の計測で「調べ尽くした真の未知」が支配的と分かってから再検討する。
