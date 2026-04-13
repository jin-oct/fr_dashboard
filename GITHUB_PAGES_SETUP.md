# GitHub Pages 配備メモ

## 構成
- `docs/index.html`
- `docs/data.json`
- `docs/.nojekyll`

## 公開手順
1. このリポジトリを GitHub に push
2. GitHub の `Settings` → `Pages`
3. `Build and deployment` の `Source` を `Deploy from a branch`
4. Branch を `main`、Folder を `/docs` に設定
5. 保存後、数分待って公開 URL を確認

## 更新手順
1. ローカルで `python generate.py`
2. `docs/index.html` と `docs/data.json` が更新される
3. commit / push

## 注意
- 簡易ロックはクライアント側ハッシュ照合です
- 本格的な認証ではありません
