from PySide6.QtWidgets import (QWidget, QVBoxLayout, QLabel, QLineEdit,
                                QPushButton, QFormLayout, QHBoxLayout,
                                QFrame, QScrollArea, QMessageBox, QProgressBar,
                                QComboBox)
from PySide6.QtCore import Qt, QThread, Signal


class SettingsView(QWidget):
    settings_saved = Signal()
    def __init__(self, config, parent=None):
        super().__init__(parent)
        self.setObjectName("SettingsView")
        self.config = config
        self._is_admin = False
        from core.updater import UpdateManager
        self.updater = UpdateManager(self)

        self.scroll = QScrollArea()
        self.scroll.setObjectName("SettingsScroll")
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QFrame.NoFrame)
        self.scroll.setStyleSheet("QScrollArea { border: none; background: transparent; }")

        container = QWidget()
        container.setObjectName("SettingsContainer")
        layout = QVBoxLayout(container)
        layout.setContentsMargins(32, 32, 32, 32)
        layout.setSpacing(24)

        # Header
        header = QLabel("⚙️  設定")
        header.setObjectName("PageHeader")
        layout.addWidget(header)

        subtitle = QLabel("Redash接続とデータ同期のパラメータを管理します")
        subtitle.setObjectName("PageSubtitle")
        layout.addWidget(subtitle)

        # ── Section 1: Redash Connection ──
        self.conn_header = self._section_header("🔗  Redash 接続設定")
        layout.addWidget(self.conn_header)
        
        self.conn_frame = self._card_frame()
        conn_layout = QFormLayout(self.conn_frame)
        conn_layout.setSpacing(14)
        conn_layout.setContentsMargins(20, 16, 20, 16)
        
        self.url_input = self._input(self.config.get("url"), "https://redash.example.com")
        self.key_input = self._input(self.config.get("key"), "APIキーを入力")
        self.key_input.setEchoMode(QLineEdit.Password)
        
        self._add_row(conn_layout, "Redash URL", self.url_input)
        self._add_row(conn_layout, "API キー", self.key_input)
        layout.addWidget(self.conn_frame)

        # ── Section 2: Query IDs ──
        self.query_header = self._section_header("📋  クエリ設定")
        layout.addWidget(self.query_header)
        
        self.query_frame = self._card_frame()
        query_layout = QFormLayout(self.query_frame)
        query_layout.setSpacing(14)
        query_layout.setContentsMargins(20, 16, 20, 16)
        
        self.q993_input = self._input(self.config.get("q993"), "993")
        self.q994_input = self._input(self.config.get("q994"), "994")
        self.q1011_input = self._input(self.config.get("q1011"), "1011")
        
        self._add_row(query_layout, "クエリID（クライアント一覧）", self.q993_input)
        self._add_row(query_layout, "クエリID（ランキング詳細）", self.q994_input)
        self._add_row(query_layout, "クエリID（ドリルダウン）", self.q1011_input)
        layout.addWidget(self.query_frame)

        # ── Section 3: Drill-down Sync Parameters ──
        layout.addWidget(self._section_header("🔄  ドリルダウン同期パラメータ"))

        sync_frame = self._card_frame()
        sync_layout = QFormLayout(sync_frame)
        sync_layout.setSpacing(14)
        sync_layout.setContentsMargins(20, 16, 20, 16)

        self.start_date_input = self._input(self.config.get("start_date"), "2024-01")
        self.end_date_input = self._input(self.config.get("end_date"), "2024-12")
        self.voucher_type_input = self._input(self.config.get("voucher_type"), "all")
        self.thread_input = self._input(str(self.config.get("threads")), "5")
        self.thread_input.setFixedWidth(80)

        self._add_row(sync_layout, "ドリルダウン開始月 (YYYY-MM)", self.start_date_input)
        self._add_row(sync_layout, "ドリルダウン終了月 (YYYY-MM)", self.end_date_input)
        self._add_row(sync_layout, "証憑タイプ (all またはカンマ区切り)", self.voucher_type_input)
        self._add_row(sync_layout, "並列取得スレッド数", self.thread_input)
        layout.addWidget(sync_frame)

        # ── Section 4: Software Update ──
        layout.addWidget(self._section_header("🆙  ソフトウェアアップデート"))
        
        update_frame = self._card_frame()
        update_layout = QVBoxLayout(update_frame)
        update_layout.setContentsMargins(20, 16, 20, 16)
        update_layout.setSpacing(12)
        
        v_row = QHBoxLayout()
        v_row.addWidget(QLabel("現在のバージョン:"))
        self.ver_label = QLabel(self.updater.CURRENT_VERSION)
        self.ver_label.setStyleSheet("font-weight: 700; color: #6366F1;")
        v_row.addWidget(self.ver_label)
        v_row.addStretch()
        update_layout.addLayout(v_row)
        
        self.update_btn = QPushButton("🚀  アップデートを確認")
        self.update_btn.setObjectName("ActionBtn")
        self.update_btn.setFixedWidth(200)
        self.update_btn.clicked.connect(self._check_updates)
        self.update_btn.setCursor(Qt.PointingHandCursor)
        update_layout.addWidget(self.update_btn)
        
        self.update_progress = QProgressBar()
        self.update_progress.setVisible(False)
        self.update_progress.setFixedHeight(6)
        self.update_progress.setTextVisible(False)
        update_layout.addWidget(self.update_progress)
        
        self.update_status = QLabel("")
        self.update_status.setStyleSheet("font-size: 11px; color: #94A3B8;")
        update_layout.addWidget(self.update_status)
        layout.addWidget(update_frame)

        # ── Section 5: Data Source Settings ──
        layout.addWidget(self._section_header("🌐  データソース設定"))
        
        src_frame = self._card_frame()
        src_layout = QFormLayout(src_frame)
        src_layout.setSpacing(14)
        src_layout.setContentsMargins(20, 16, 20, 16)
        
        self.source_combo = QComboBox()
        self.source_combo.addItem("ローカルファイル (標準)", "local")
        self.source_combo.addItem("ウェブ参照 (GitHub / Google Drive)", "remote")
        
        idx = self.source_combo.findData(self.config.get("data_source", "local"))
        if idx >= 0: self.source_combo.setCurrentIndex(idx)
        
        self._add_row(src_layout, "データ取得モード", self.source_combo)
        layout.addWidget(src_frame)
        
        # Remote URLs in its own frame for hiding
        self.remote_url_frame = self._card_frame()
        remote_url_layout = QFormLayout(self.remote_url_frame)
        remote_url_layout.setSpacing(14)
        remote_url_layout.setContentsMargins(20, 16, 20, 16)
        
        self.ranking_url_input = self._input(self.config.get("remote_ranking_url"), "https://raw.githubusercontent.com/.../ranking.csv")
        self.dd_url_input = self._input(self.config.get("remote_drilldown_url"), "https://raw.githubusercontent.com/.../drilldown.csv")
        
        self._add_row(remote_url_layout, "ランキング詳細 URL", self.ranking_url_input)
        self._add_row(remote_url_layout, "ドリルダウン URL", self.dd_url_input)
        layout.addWidget(self.remote_url_frame)

        # ── Save Button ──
        btn_layout = QHBoxLayout()
        self.save_btn = QPushButton("💾  設定を保存")
        self.save_btn.setObjectName("PrimaryBtn")
        self.save_btn.setFixedWidth(200)
        self.save_btn.setFixedHeight(40)
        self.save_btn.clicked.connect(self.save)
        self.save_btn.setCursor(Qt.PointingHandCursor)
        btn_layout.addWidget(self.save_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        layout.addStretch()
        self.scroll.setWidget(container)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(self.scroll)

    def set_theme(self, is_dark):
        bg = "#F8FAFC" if not is_dark else "#0F172A"
        self.setStyleSheet(f"QWidget#SettingsView {{ background-color: {bg}; }}")
        if hasattr(self, "scroll"):
            self.scroll.setStyleSheet(f"QScrollArea#SettingsScroll {{ background-color: {bg}; border: none; }}")
        
        container = self.findChild(QWidget, "SettingsContainer")
        if container:
            container.setStyleSheet(f"QWidget#SettingsContainer {{ background-color: {bg}; }}")

    # ── Helpers ──

    def _section_header(self, text):
        lbl = QLabel(text)
        lbl.setObjectName("DetailSubHeader")
        return lbl

    def _card_frame(self):
        frame = QFrame()
        frame.setObjectName("FilterFrame")
        return frame

    def _input(self, value, placeholder=""):
        inp = QLineEdit(value or "")
        inp.setPlaceholderText(placeholder)
        # Styling handled by global QSS
        return inp

    def _add_row(self, form_layout, label_text, widget):
        lbl = QLabel(label_text)
        lbl.setObjectName("FilterLabel")
        form_layout.addRow(lbl, widget)

    def save(self):
        # Basic validation
        url = self.url_input.text().strip()
        if url and not (url.startswith("http://") or url.startswith("https://")):
            QMessageBox.warning(self, "入力エラー", "Redash URLは http:// または https:// で始まる必要があります。")
            return

        try:
            threads = int(self.thread_input.text().strip() or "5")
            if threads < 1 or threads > 20:
                raise ValueError()
        except ValueError:
            QMessageBox.warning(self, "入力エラー", "スレッド数は1〜20の整数を入力してください。")
            return

        new_data = {
            "url": url,
            "key": self.key_input.text().strip(),
            "q993": self.q993_input.text().strip(),
            "q994": self.q994_input.text().strip(),
            "q1011": self.q1011_input.text().strip(),
            "start_date": self.start_date_input.text().strip(),
            "end_date": self.end_date_input.text().strip(),
            "voucher_type": self.voucher_type_input.text().strip(),
            "threads": threads,
            "data_source": self.source_combo.currentData(),
            "remote_ranking_url": self.ranking_url_input.text().strip(),
            "remote_drilldown_url": self.dd_url_input.text().strip()
        }
        self.config.save(new_data)
        self.settings_saved.emit()

        # Visual feedback
        self.save_btn.setText("✅  保存完了!")
        self.save_btn.setObjectName("SuccessBtn")
        self.save_btn.style().unpolish(self.save_btn)
        self.save_btn.style().polish(self.save_btn)

        from PySide6.QtCore import QTimer
        QTimer.singleShot(2000, self._reset_save_btn)

    def _reset_save_btn(self):
        self.save_btn.setText("💾  設定を保存")
        self.save_btn.setObjectName("PrimaryBtn")  # Reset to default primary
        self.save_btn.style().unpolish(self.save_btn)
        self.save_btn.style().polish(self.save_btn)

    def set_admin_mode(self, is_admin):
        """Toggle visibility of sensitive settings."""
        self._is_admin = is_admin
        
        # Toggle Visibility of Sensitive Sections
        self.conn_header.setVisible(is_admin)
        self.conn_frame.setVisible(is_admin)
        
        self.query_header.setVisible(is_admin)
        self.query_frame.setVisible(is_admin)
        
        self.remote_url_frame.setVisible(is_admin)
        
        # Data Source Mode (Source Combo) remains ALWAYS visible and enabled
        self.source_combo.setEnabled(True)
        
        # Mask API Key even when visible
        self.key_input.setEchoMode(QLineEdit.Password)

    # ── Update Logic ──
    def _check_updates(self):
        self.update_btn.setEnabled(False)
        self.update_status.setText("サーバーを確認しています（数秒かかる場合があります）...")
        
        # Move to a thread to prevent UI hang on corporate networks
        class UpdateThread(QThread):
            finished = Signal(tuple)
            def __init__(self, manager):
                super().__init__()
                self.manager = manager
            def run(self):
                res = self.manager.check_for_updates()
                self.finished.emit(res)

        self.check_thread = UpdateThread(self.updater)
        self.check_thread.finished.connect(self._on_check_finished)
        self.check_thread.start()

    def _on_check_finished(self, result):
        available, latest, url, changelog = result
        self.update_btn.setEnabled(True)
        
        if available:
            msg = f"新しいバージョン ({latest}) が見つかりました。\n\n【更新内容】\n{changelog}\n\n今すぐアップデートしますか？"
            reply = QMessageBox.question(self, "アップデートあり", msg, QMessageBox.Yes | QMessageBox.No)
            if reply == QMessageBox.Yes:
                self._start_download(url)
            else:
                self.update_status.setText(f"更新がキャンセルされました (最新: {latest})")
        else:
            if "エラー" in changelog:
                self.update_status.setText(f"❌ {changelog}")
            else:
                self.update_status.setText(f"✅ 最新バージョン ({self.updater.CURRENT_VERSION}) を使用中です。")

    def _start_download(self, url):
        self.update_progress.setVisible(True)
        self.update_progress.setValue(0)
        self.update_status.setText("ダウンロード中...")
        self.update_btn.setEnabled(False)
        
        # Threaded download to prevent UI hang
        class DownloadThread(QThread):
            progress = Signal(int, str)
            finished = Signal(bool)
            def __init__(self, manager, url):
                super().__init__()
                self.manager = manager
                self.url = url
            def run(self):
                # Connect internal manager signal to this thread's signal
                self.manager.progress.connect(self.progress.emit)
                res = self.manager.download_update(self.url)
                self.finished.emit(res)

        self.dl_thread = DownloadThread(self.updater, url)
        self.dl_thread.progress.connect(self._on_download_progress)
        self.dl_thread.finished.connect(self._on_download_finished)
        self.dl_thread.start()

    def _on_download_finished(self, success):
        self.update_btn.setEnabled(True)
        if success:
            self.update_status.setText("ダウンロード完了。再起動して適用します...")
            reply = QMessageBox.information(self, "更新準備完了", 
                                          "アップデートの準備ができました。アプリを終了して更新を適用します。", 
                                          QMessageBox.Ok)
            if self.updater.apply_update():
                from PySide6.QtWidgets import QApplication
                QApplication.quit()
        else:
            self.update_progress.setVisible(False)
            self.update_status.setText("❌ ダウンロードに失敗しました。")

    def _on_download_progress(self, pct, msg):
        self.update_progress.setValue(pct)
        self.update_status.setText(msg)
