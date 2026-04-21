# 自動アップデート機能の運用手順

本アプリには Google ドライブを活用した自動アップデート機能が組み込まれました。

## 開発者側の準備（リリースの手順）

1.  **アプリのビルド**
    `build.bat` を実行して、最新の `main.dist` を生成してください。

2.  **アップデート用ZIPの作成**
    `main.dist` フォルダーの中身をすべて `update.zip` という名前で圧縮してください。
    (注意: `main.dist` フォルダー自体を固めるのではなく、**その中身（exeファイル等がある階層）**を直接固めてください)

3.  **メタデータの作成 (`version.json`)**
    以下の形式で `version.json` を作成してください。
    ```json
    {
      "version": "2.1.3",
      "url": "https://drive.google.com/uc?export=download&id=YOUR_FILE_ID",
      "changelog": "・自動アップデート機能を追加\n・UIの改善"
    }
    ```
    - `version`: 現在のバージョンより高い数字に設定。
    - `url`: `update.zip` を Google ドライブにアップロードし、その「公開リンク」の ID 部分を上記 URL に当てはめてください。

4.  **ファイルの公開**
    `update.zip` と `version.json` を以下の Google ドライブフォルダーにアップロードしてください。
    - **フォルダー**: `Streamed BI > update`
    - **URL**: [Google Drive Folder](https://drive.google.com/drive/folders/1PbClHeq9_gVBzk_m2K2ktvuY0ykHXdlC)
    - アップロード後、各ファイルを「リンクを知っている全員に閲覧（公開）」設定にするのを忘れないでください。

5.  **アプリ側のURL設定**
    `core/updater.py` 内の `VERSION_URL` を、作成した `version.json` の直リンクに書き換えてビルドしてください。

## ユーザー側の操作
1.  「設定」タブを開く。
2.  「アップデートを確認」ボタンを押す。
3.  更新があればダイアログが出るので、「はい」を押してダウンロード。
4.  ダウンロード完了後、アプリを閉じると自動的にファイルが差し替えられ、再起動します。

---
※この機能により、今後はインストーラー (`Setup.exe`) を毎回配らなくても、ZIPをドライブに置くだけでユーザーに修正を届けることが可能になります。
