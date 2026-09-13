# data/ の読み方（機械読み向けの説明・層1）

このリポジトリの `data/` に置かれた JSON / XML は、**認証なしで誰でも読める読み取り専用のデータ**です。
このファイルは、**そのデータだけを見て中身を正しく解釈するための説明**です。コードを読まなくても、この1枚で `upcoming.json` の1行が読めることを目標にしています。

- **版**＝v1.1（2026-09-13 作成・`0913-1152`。v1.0 を実物の1行で自己監査し、欠落8件を修理した版）
- **設計の正**＝Notion「game-site 土台」`0810-1129`（本人確定 `-07`／骨子 `-11`）。このファイルは正ではなく、**正から派生した説明**です。
- **この時点では第三者への再配布を想定していません。** 詳細は §8。

---

## 0. 先に読む4つの約束

1. **空欄は「無い」を意味しません。** `null` は多くの場合「まだ調べていない」「調べられなかった」です。「調べた結果ゼロだった」とは区別されています（§5）。
2. **予想は外れます。** `spike_prob` は自社実績で較正したモデルの出力で、**上位帯は自信過剰であることが実測されています**（§6）。
3. **入れ物の形が3種類あります。** 行の配列名がファイルによって `rows` / `items` / `features` と違います（§2）。
4. ★**データ内の時刻はすべて UTC です。** `generated_at` / `observed_at` / `history` の時刻は UTC（`…+00:00` または `…Z`）。**一方、§1 の更新時刻の表は JST です。**混ぜると9時間ずれます。

---

## 1. 公開URLと更新時刻

機械読みには **`raw.githubusercontent.com`** を使ってください。公開サイトと同じ GitHub Pages から読むこともできますが、**Pages は公開サイト本体と帯域を共有する**ため、機械読みは raw 側に寄せます。

ベースURL＝`https://raw.githubusercontent.com/gakinotukaida-dot/game-site/main/data/`

| ファイル | 中身 | 行の入れ物 | 件数※ | schema | 更新（JST） |
|---|---|---|---|---|---|
| `upcoming.json` | 発売前の羽根予想 | `rows` | 200 | `upcoming_v5` | 01:45・13:45 |
| `view02_rising.json` | 伸びている作品の検知と原因調査 | `items` | 30 | `meta.schema_version: 1` | 04:18・10:18・16:18・22:18 |
| `now_ccu.json` | 現在の同時接続数の上位 | `rows` | 100 | `v5-hist` | 04:17・10:17・16:17・22:17 |
| `new_rising.json` | 新作で伸びているもの | `rows` | 38 | `v5-hist` | 02:20・14:20 |
| `free_promos.json` | 無料配布・無料化の情報 | （配列なし・`counts`/`stores`） | — | `free_promos_v1` | 04:17・10:17・16:17・22:17 |
| `prelaunch_model.json` | 羽根予想モデルの中身（重みと較正） | `features` | 11 | `prelaunch_model_v1` | 01:20・13:20 |
| `prediction_scorecard.json` | 予想の答え合わせ（較正表） | （配列なし・`holdout`/`live`） | — | `prediction_scorecard_v1` | 14:55 |
| `prelaunch_backtest.json` | 発売前シグナルの検証 | （配列なし） | — | `prelaunch_backtest_v1` | 日曜 14:35 |
| `rising_feed.xml` | 上記を人向けに流す RSS | RSS 2.0 | — | — | 04:25・10:25・16:25・22:25 |

※件数は 2026-09-13 時点の実測です。**件数は日によって変わります**（§6）。
※更新時刻は GitHub Actions の cron（UTC）から換算した**予定時刻**で、実際の書き込みは数分ずれます。**失敗した回はファイルが更新されないだけで、古いファイルがそのまま残ります。** 鮮度は必ず各ファイルの `generated_at`（または `observed_at`）で判断してください。**その値は UTC です**（表の時刻は JST）。

---

## 2. 共通の形

```
{ "view": …, "schema": …, "generated_at": …, "count": …, "rows": [ … ] }
```

これが基本形ですが、**2つだけ形が違います**。機械読みでは分岐が必要です。

- **`view02_rising.json`**＝行の配列名が **`items`**、メタは **`meta` の下**、`view` キーが**ありません**。トップ直下に `baseline_mode` と `up_thresh` が付くことがあります（§4）。
- **`prelaunch_model.json`**＝配列名が **`features`** で、これは「作品の一覧」ではなく**モデルの特徴量の一覧**です。他のファイルの `rows` と同じものとして扱わないでください。

形を統一する予定は現時点ではありません。**この説明ファイル側で吸収する**方針です。

### 2-1. `upcoming.json` のトップレベル（行を読むのに要るもの）

| キー | 意味 |
|---|---|
| `generated_at` | 生成時刻（**UTC**） |
| `count` | `rows` の件数 |
| `note` | このファイル自身の注意書き（人間向け） |
| `view` / `schema` / `source` | データの種類名／スキーマ名／材料の出所を1行で書いたもの |
| `params` | 抽出条件（`limit`・`genre_max`・`app_types`） |
| **`model`** | **行の `spike_prob` を作ったモデルの素性。** `base_rate`＝全体の跳ね率（`expect` の「基準率」はこれ）／`hit_threshold`＝**跳ねの定義**（現在 1000人）／`readiness`＝`validated` なら検証済み／`validation_oos.top_decile_lift`＝上位10%の的中倍率 |

**`rows` は `spike_prob` の高い順**に並んでいます（`null` の作品は `expect` → 発売日で代替）。

---

## 3. `upcoming.json` の行（全27キー）

発売前（`coming_soon`）の作品を、羽根予想の高い順に200件。

### 3-1. 作品の識別と発売日

| キー | 型 | 意味 |
|---|---|---|
| `appid` | 整数 | Steam の appid。作品の一意キー |
| `name` | 文字列 | 作品名 |
| `release` | 文字列 / null | 発売日。**型が2通りある**（下記） |
| `release_known` | 真偽 | `release` が**確定日か**を示す。`true`＝`release` は ISO 形式（`2026-09-16`）。`false`＝`release` は **Steam の表示文字列そのまま**（`16 Sep, 2026`・`Jul 2026`・`2026` など粒度がまちまち） |
| `coming_soon` | 真偽 | Steam 側の「近日公開」フラグ。この一覧は全件 `true` |

> **`release` を日付としてパースする前に、必ず `release_known` を見てください。** `false` のときは粒度が日・月・年のいずれかで、パースに失敗しうります。

### 3-2. 羽根予想

| キー | 型 | 意味 |
|---|---|---|
| `spike_prob` | 0〜1 / null | **跳ね確率**＝発売直後14日で**最大同接が `model.hit_threshold`（現在1,000）人以上**になる確率。モデルが使えない作品は `null`。★**額面通りに読まないこと → §6-2 を必ず参照** |
| `expect` | `high`/`mid`/`low` | 表示用の粗い3段。`spike_prob` が**基準率（`model.base_rate`）**の何倍か（3倍以上＝`high`／1.5倍以上＝`mid`／それ未満＝`low`）。`spike_prob` が `null` の作品は体験版の同接とニュースの有無で代替 |
| `conf` | `high`/`mid`/`low`/`na` | **予測の確からしさ。** `na`＝`spike_prob` が無い／`high`＝`model.readiness` が `validated` かつ `active_signals` が2以上／`mid`＝`active_signals` が1以上／`low`＝それ以外 |
| `active_signals` | 整数 | その作品で実際に効いたシグナルの本数。**0 なら予想はほぼ基準率に寄っている**（材料が無い） |
| `factors` | 配列 | 確率を**押し上げた**要因の上位3つまで。下記 |

`factors` の各要素は `{"name", "dir", "bucket"}`。

- `name`＝要因の名前。実測の値域＝`is_free` / `dev_best_peak` / `dev_best_reviews` / `genre` / `news_count` / `demo_ccu` / `web_reach` / `web_news`
- `dir`＝向き。**押し上げ要因だけを出しているので、実質つねに `up` です。** `dir` を見て判断する読み方はできません（**押し下げ要因はこのデータに出てきません**）
- `bucket`＝その要因のどの帯で効いたか。**数値の大小帯（`high`/`mid`/`low`）とカテゴリ値（`paid`/`hot`）が混在します。** 例＝`{"name":"is_free","bucket":"paid"}` は「**無料ではない＝有料であることが押し上げ要因**」の意味

> ★**`spike_prob` を単独で報告しないでください。** 例えば `spike_prob: 0.9883`（98.8%）という行が実在しますが、**40%以上と予測した群の実測は35.1%です**（§6-2）。高い確率ほど額面より低く出ます。

### 3-3. 実測シグナルの生値（監査用）

| キー | 型 | 意味 |
|---|---|---|
| `demo_ccu` | 整数 / null | 体験版の同時接続数。**null＝体験版が無い、ではなく未取得** |
| `twitch_peak` | 整数 / **null** | Twitch の視聴ピーク。★**null＝未取得。** 現在は全件 null です（§6-1） |
| `streamers` | 整数 | 配信者の数。★**こちらは `0` が入ります＝「調べて0人」。** 上の `twitch_peak: null`（未取得）と**意味が違います** |
| `news_count` / `has_news` | 整数 / 真偽 | 告知・ニュースの本数と有無 |
| `dev_best_peak` / `dev_best_reviews` | 整数 / null | 開発元の過去作の最高同接／レビュー数 |
| `web_news` | 整数 / null | 世界の多言語ニュース記事数（GDELT） |
| `web_views` | 整数 / null | 全言語版 Wikipedia の直近ページビュー合計 |
| `web_reach` | 整数 / null | 言語版 Wikipedia の数（Wikidata） |

> **`web_news` / `web_views` / `web_reach` は設計上ほとんど埋まりません。** 外部APIの制限と対象抽出の偏りによるもので、**空欄は「話題になっていない」を意味しません**。これらを理由づけに使わないでください。

### 3-4. 付帯情報

| キー | 型 | 意味 |
|---|---|---|
| `is_free` | 真偽 | 無料タイトルか |
| `header_image` | 文字列 / null | サムネの正しいURL。**null は「未取得」で、URLを自分で組み立てても当たりません**（Steam 側がハッシュ付きパスを使うため） |
| `has_japanese` | 真偽 / **null** | 日本語対応。**三値です。`null`＝まだ調べていない。`false` と混ぜないでください** |
| `genres` | 配列 | ジャンルタグ。未取得は空配列 |
| `price` | オブジェクト / null | 価格。**null＝未取得であって「0円」ではありません**（無料かどうかは `is_free` を見る） |
| `review` | オブジェクト / null | レビュー集計。下記 |
| `detail` | オブジェクト | 詳細ページ用の情報。§3-5 |

`review` の中身は `{"positive", "total", "pct"}` です。

- `review` 自体が **`null`＝未取得**（「レビューが無い」ではない）
- `total: 0`＝**取得できて、レビューが実際に0件**
- ★**`pct: null` は「好評率0%」ではありません。** 母数が0で割合を計算できない、という意味です。`positive: 0, total: 0, pct: null` はごく普通に出ます（発売前なので当然）

### 3-5. `detail` の中身

詳細ページ用の入れ物で、キーは `developers` / `publishers` / `genres` / `categories` / `dlc_count` / `release` / `website` / `siblings` / `price` / `review` です。

- ★**`siblings` は「この作品」の数字ではありません。** 同じ開発元の**別の作品**とその現在CCU（`{"appid","name","ccu"}` の配列）です。この作品のCCUではありません（発売前なので存在しません）。
- **`detail.price` / `detail.review` / `detail.release` は、行のトップレベルと同じ値の写しです。** 食い違ったときは**トップレベルを正**としてください。
- `dlc_count` は DLC の本数。`website` は公式サイト（未取得は `null`）。

---

## 4. `view02_rising.json` の行

伸びている作品30件と、**その原因を実際に調べた記録**。

- `rank` / `appid` / `name`
- `detection`＝検知の数値。`current_ccu`・`recent_value`（直近の代表値）・`baseline`（平常値）・`ratio`（＝いつもより何倍）・`robust_z`・`n_points`・`is_riser`・`is_launch`。**`recent_value` / `baseline` / `ratio` / `robust_z` は null になりえます**（材料不足の回）。
- `al`（トップに `baseline_mode: "hour_matched"` があるときだけ出る）＝**`1`＝分母に「同じ時間帯の平常値」を使えた**／**`0`＝日数不足で従来の平常値を使い、確度を一段下げた**。`0` の行は `ratio` の比較可能性が落ちます。
- `up_thresh`（トップ）＝「いつもより」と強調するしきい値。
- `signals` / `prediction`＝見つかった原因の種別（セール・ニュース・新作・無料化・レビュー・日本語ニュース・配信など）。**個人は含みません。**
- `investigation`＝**何を調べ、当たったか外れたか、なぜ不明なのか**（§5）。
- `confidence` / `score` / `history`（`[[時刻, 同接], …]` の6時間バケット）。

`meta` には `schema_version`・`generated_at`（**UTC**）・`method`・`experimental: true`・`disclaimer_code: "provisional_weights_experimental"`・`window`（平常値の取り方）・`item_count` が入ります。**`experimental: true` は「重み付けが暫定であり、そのまま断定に使うな」という意味です。**

---

## 5. 「調べていない」と「調べて陰性」を混ぜないための規則

このデータは**照会失敗と陰性を必ず区別**します。読む側もこの区別を落とさないでください。

`view02_rising.json` の `investigation.unknown_reason` は次の値を取ります（`null` を含めて5通り）。

| 値 | 意味 |
|---|---|
| `null` | 原因が特定できている（`signals` を見る） |
| `investigated_all_negative` | **調べ尽くして陰性**＝正直な「原因不明」 |
| `web_query_failed` | Web調査が失敗した＝**調べ尽くしたとは言えない** |
| `web_skipped_budget` | 主要手段を予算の都合で未照会＝カバレッジ欠落 |
| `twitch_key_absent` | 鍵が無く一部を未実施＝完全な調べ尽くしではない |

同じ考え方が `upcoming.json` の `has_japanese`（三値）、`price` / `review` / `header_image`（null＝未取得）にも適用されています。

---

## 6. 参考値（★この節だけが古くなります・測定日つき）

> **この節の数値は 2026-09-13 に測定したスナップショットです（対象データの生成は 2026-09-12）。**
> 数値は日々変わります。**最新を知りたいときは、この節を信じず、右列のURLとキーを直接読んでください。**
> この説明ファイルで**腐るのはこの節だけ**になるように設計しています。

### 6-1. `upcoming.json` の充足率（n=200・非nullの件数）

| キー | 実測 | キー | 実測 |
|---|---|---|---|
| `appid` / `name` / `spike_prob` / `expect` / `conf` / `active_signals` / `news_count` / `streamers` / `is_free` / `coming_soon` / `detail` | 200 | `review` | 195 |
| `genres`（非空） | 184 | `release`（非null） | 184 |
| `header_image` | 184 | `has_japanese` | 184（true 84・false 100・null 16） |
| `dev_best_reviews` | 67 | `dev_best_peak` | 61 |
| `has_news`（true） | 45 | `demo_ccu` | 35 |
| `web_reach` | 34 | `web_news` | 25 |
| `web_views` | 6 | `price` | **0** |
| `twitch_peak` | **0** | `release_known`（true） | **0** |

最新＝`.../data/upcoming.json` を取得し、`rows` を数える。

### 6-2. 予想の性能（★単独の倍率だけで語らないこと）

| 指標 | 実測 | 取得先 |
|---|---|---|
| 上位10%の的中倍率 | **×5.96**（out-of-sample・評価 n=1,678） | `prelaunch_model.json` の `validation_oos.top_decile_lift` |
| **上位帯（40%以上と予測した群）の較正** | **予測平均 75.0% に対し、実測 35.1%（n=74）** | `prediction_scorecard.json` の `holdout.calibration` の最終行 |

> **上位帯は自信過剰です。**「75%と言った群が実際は35%」。**倍率（×5.96）だけを単独で示さず、必ずこの較正を併記してください。**
> 較正表は6段（0–2%／2–5%／5–10%／10–20%／20–40%／40%以上）あり、`holdout` が過去データの70/30分割、`live` が発売前に記録した本番の予測です。`live` はまだ解決済み2件で、**判断に使える量ではありません**。

---

## 7. 安定の約束と、約束しないこと

**約束すること**

- 上記のURL（ファイル名とパス）は変えません。変える場合は旧URLを残します。
- 既存キーの**意味を黙って変えません**。
- `schema` / `schema_version` を上げずに破壊的変更をしません。

**約束しないこと**

- **件数・充足率は保証しません**（§6）。
- **キーの追加はします**（`al` や `up_thresh` のように後から増えます）。未知のキーを許容する読み方にしてください。
- **更新の成功は保証しません**。失敗した回は古いファイルが残ります。必ず `generated_at` を見てください。
- 履歴は保持しません。各ファイルは**毎回上書き**です。

---

## 8. 出典と再配布について

データの出所は Steam のストア情報、Wikipedia / Wikidata、GDELT、各種RSSなどです。**Steam のストア情報は公式に文書化されたWeb APIではない経路を含みます。**

**この時点では、第三者がこのデータを取得して再配布・再加工することは想定していません。** 個人の利用を超える利用を検討する場合は、各出典の利用規約をご自身で確認してください。**ここに書いた内容は下調べであり、法的な助言ではありません。**
