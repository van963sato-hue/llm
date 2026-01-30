# LLM Memory Album (GitHub Pages)

ChatGPTなどのログを「アルバム」っぽくローカル保存して眺めるための静的Webアプリです。

## 特徴
- 端末内保存（IndexedDB）。サーバーに送信しません。
- ChatGPT Export ZIP（conversations.json入り） / conversations.json単体 / 汎用JSONを取り込み可能。
- 「Moment」（一瞬ログ抜粋）に、イラスト・日付・タグ・当時の相棒プロンプト・お別れメッセージを付与。
- 「Timeline」で節目と「その時点の記憶メモ」を自由入力で保存。
- provider/model フィルタで絞り込み。
- Momentの表紙トリミング（中央トリム 1:1 / 3:4 / 16:9）。
- お別れメッセージのテンプレ挿入。
- 検索はWeb Workerでインデックス構築（UI停止しにくい）。
- エクスポート：
  - 軽量（json）：History/Moment/プロンプト/ラベル（確実に落とせる）
  - 完全（zip）：会話＋画像＋メタ
  - 画像だけ（zip）

## GitHub Pages 配布
1. このフォルダの中身をそのまま GitHub リポジトリのルートに置いて push。
2. GitHub の Settings → Pages → Deploy from a branch で `main / (root)` を選ぶ。
3. 表示されたURLにアクセス。

## 取り込み
- ChatGPT: Settings > Data Controls > Export data でZIPを受け取り、ZIPを選択して取り込み。
- ZIPが重い場合：PCでZIPを展開して `conversations.json` を取り込むと安定します。

## 汎用JSON形式（他社ログ）
最終的にこの形に近いJSONなら取り込めます：

{
  "sessions": [
    {
      "provider": "anthropic",
      "id": "session-001",
      "title": "…",
      "createdAt": 1730000000000,
      "updatedAt": 1730000000000,
      "messages": [
        {"role":"user","text":"...","ts":1730000000,"model":"..."},
        {"role":"assistant","text":"...","ts":1730000001,"model":"..."}
      ]
    }
  ]
}

## 注意
- GitHub Pagesは公開URLです。ログや画像をリポジトリに入れないでください（このアプリは各端末で取り込みます）。
