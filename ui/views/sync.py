from PySide6.QtWidgets import (QWidget, QVBoxLayout, QPushButton, QLabel,
                               QProgressBar, QTextEdit, QHBoxLayout, QFileDialog,
                               QFrame, QGridLayout)
from PySide6.QtCore import Qt, QThread, Signal
from PySide6.QtGui import QTextCursor
import asyncio
import os
import json
import time
from core.redash import RedashClient


class SyncWorker(QThread):
    progress = Signal(int, int)
    rows_updated = Signal(int, int)
    log = Signal(str)
    finished = Signal()
    error = Signal(str)

    def __init__(self, config, engine, target="full", sync_type="diff"):
        super().__init__()
        self.config = config
        self.engine = engine
        self.target = target # "full", "drilldown", "client"
        self.sync_type = sync_type # "diff", "clean"
        self._is_running = True
        self._active_tasks = set()
        self._loop = None
        # Optimization: Buffering to reduce append transitions
        self._ranking_buffer = []
        self._dd_buffer = []
        self._buffer_threshold = 2500 # Flush every ~2500 rows

    def stop(self):
        self._is_running = False
        if self._loop and self._active_tasks:
            # We must use call_soon_threadsafe or similar if the loop is in another thread
            for task in list(self._active_tasks):
                self._loop.call_soon_threadsafe(task.cancel)

    def run(self):
        try:
            asyncio.run(self.do_sync())
        except Exception as e:
            self.error.emit(str(e))
        finally:
            self.finished.emit()

    async def do_sync(self):
        rc = RedashClient(self.config.get("url"), self.config.get("key"))
        q993 = self.config.get("q993")
        q994 = self.config.get("q994")
        
        if self.sync_type == "clean":
            self.log.emit("[ENGINE-DB] Full Sync selected. Resetting local database...")
            self.engine.reset_data()

        self.log.emit("[REDASH] Fetching active client list...")
        clients = await rc.fetch_query(q993)
        total = len(clients)
        self.log.emit(f"[REDASH] Found {total:,} clients.")
        
        # Build ID-to-Name map for local joining
        name_map = {}
        for c in clients:
            cid = str(c.get("client_id", ""))
            cname = c.get("enterprise_name") or c.get("client_name") or c.get("name") or ""
            if cid:
                name_map[cid] = cname

        self.total_ranking_rows = 0
        self.total_drilldown_rows = 0
        self.progress.emit(0, total)
        
        chunk_size = 50
        done = 0
        threads = self.config.get("threads", 5)
        sem = asyncio.Semaphore(threads)

        async def fetch_chunk(chunk, chunk_num, total_chunks):
            if not self._is_running: return
            
            async with sem:
                params = {f"id{idx+1}": str(c.get("client_id")) for idx, c in enumerate(chunk)}
                for idx in range(len(chunk), 50):
                    params[f"id{idx+1}"] = "0"

                try:
                    start_time = time.time()
                    self.log.emit(f"[REDASH] Chunk {chunk_num}: Start fetching...")
                    
                    # 1. Fetch Rankings (Client data)
                    if self.target in ("full", "client"):
                        data_994 = await rc.fetch_query(q994, parameters=params)
                        
                        # Local Join: Inject company name if missing in Query 994
                        for row in data_994:
                            cid = str(row.get("client_id") or row.get("クライアントID") or "")
                            if cid in name_map:
                                if "enterprise_name" not in row:
                                    row["enterprise_name"] = name_map[cid]
                                if "企業名" not in row:
                                    row["企業名"] = name_map[cid]
                        
                        self._ranking_buffer.extend(data_994)
                    else:
                        data_994 = []
                    
                    # 2. Fetch Drill-downs (1011)
                    if self.target in ("full", "drilldown"):
                        dd_params = params.copy()
                        dd_params.update({
                            "start_date": self.config.get("start_date", "2024-01"),
                            "end_date": self.config.get("end_date", "2024-12"),
                            "voucher_type": self.config.get("voucher_type", "all"),
                            "item_filter": self.config.get("item_filter", "overall")
                        })
                        data_1011 = await rc.fetch_query(self.config.get("q1011"), parameters=dd_params)
                        self._dd_buffer.extend(data_1011)
                    else:
                        data_1011 = []
                    
                    elapsed = time.time() - start_time
                    self.total_ranking_rows += len(data_994)
                    self.total_drilldown_rows += len(data_1011)
                    self.log.emit(f"[REDASH] Chunk {chunk_num}: Done! (Rows: {len(data_994)+len(data_1011)}, Time: {elapsed:.1f}s)")
                        
                    # 3. Dynamic Flush to Engine
                    if len(self._ranking_buffer) >= self._buffer_threshold or len(self._dd_buffer) >= self._buffer_threshold:
                        if self._ranking_buffer:
                            self.engine.append_data(self._ranking_buffer, is_drilldown=False, sync_type=self.sync_type, progress_callback=self.log.emit)
                            self._ranking_buffer = []
                        if self._dd_buffer:
                            self.engine.append_data(self._dd_buffer, is_drilldown=True, sync_type=self.sync_type, progress_callback=self.log.emit)
                            self._dd_buffer = []
                        self.rows_updated.emit(self.total_ranking_rows, self.total_drilldown_rows)

                except Exception as e:
                    self.log.emit(f"[ERR] Chunk {chunk_num} failed: {e}")

                nonlocal done
                done += len(chunk)
                self.progress.emit(done, total)

        self._loop = asyncio.get_running_loop()
        
        total_chunks = (total + chunk_size - 1) // chunk_size
        pending = set()
        
        for i in range(0, total, chunk_size):
            if not self._is_running:
                break
                
            chunk = clients[i:i + chunk_size]
            chunk_num = i // chunk_size + 1
            
            task = asyncio.create_task(fetch_chunk(chunk, chunk_num, total_chunks))
            pending.add(task)
            self._active_tasks.add(task)
            task.add_done_callback(lambda t: (pending.discard(t), self._active_tasks.discard(t)))

            # Control concurrency: Wait if we have too many pending tasks
            if len(pending) >= threads:
                done_tasks, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if not self._is_running:
                    break

        # Wait for remaining tasks
        if pending:
            await asyncio.wait(pending)

        # FINAL FLUSH
        if self._ranking_buffer:
            self.engine.append_data(self._ranking_buffer, is_drilldown=False, sync_type=self.sync_type, progress_callback=self.log.emit)
        if self._dd_buffer:
            self.engine.append_data(self._dd_buffer, is_drilldown=True, sync_type=self.sync_type, progress_callback=self.log.emit)
        self.rows_updated.emit(self.total_ranking_rows, self.total_drilldown_rows)

        self.log.emit("[MERGE] Deduplicating data...")
        self.engine.deduplicate()
        
        self.log.emit("[SAVE] Exporting database to optimized Parquet and CSV... this may take a moment.")
        ok_pq = self.engine.save_to_parquet()
        ok_csv = self.engine.save_to_csv()
        ok = ok_pq and ok_csv
        
        if ok:
            if self._is_running:
                self.log.emit("[DONE] Synchronization complete! Saved to Parquet and CSV.")
            else:
                self.log.emit("[STOP] Stopped by user. Partial data preserved as Parquet and CSV.")
        else:
            self.log.emit("[WARN] Sync complete/stopped, but one or more save formats (Parquet/CSV) failed.")


class DataLoadWorker(QThread):
    """Background thread for loading ZIP/CSV."""
    log = Signal(str)
    finished = Signal(bool)

    def __init__(self, engine, mode, path=None):
        super().__init__()
        self.engine = engine
        self.mode = mode  # "zip", "csv", "reload"
        self.path = path

    def run(self):
        try:
            if self.mode == "zip":
                self.log.emit(f"[LOCAL-ZIP] Reading archive: {self.path}")
                ok = self.engine.load_from_zip(self.path, progress_callback=lambda m: self.log.emit(f"[LOCAL-ZIP] {m}"))
                if ok:
                    self.log.emit(f"[LOCAL-ZIP] DONE! Imported {self.engine.get_row_count():,} rows.")
                else:
                    self.log.emit("[LOCAL-ZIP] ERROR: Load failed.")
                self.finished.emit(ok)

            elif self.mode == "csv":
                import shutil
                self.log.emit(f"[LOCAL-CSV] Reading file: {self.path}")
                if os.path.abspath(self.path) != os.path.abspath(self.engine.master_csv):
                    shutil.copy2(self.path, self.engine.master_csv)
                ok = self.engine.reload_master_data(progress_callback=lambda m: self.log.emit(f"[LOCAL-CSV] {m}"))
                if ok:
                    self.log.emit(f"[LOCAL-CSV] DONE! Imported {self.engine.get_row_count():,} rows.")
                self.finished.emit(ok)

            elif self.mode == "reload":
                self.log.emit("[INFO] Reloading database...")
                ok1 = self.engine.reload_master_data(progress_callback=lambda m: self.log.emit(f"[ENGINE] {m}"))
                ok2 = self.engine.reload_drilldown_data(progress_callback=lambda m: self.log.emit(f"[ENGINE] {m}"))
                
                # If both are missing but zip exists, try zip
                if not self.engine.has_data():
                    if os.path.exists(self.engine.zip_path):
                        ok = self.engine.load_from_zip(self.engine.zip_path, progress_callback=lambda m: self.log.emit(f"[ENGINE] {m}"))
                        self.finished.emit(ok)
                        return

                self.log.emit(f"[DONE] Reload complete.")
                self.finished.emit(ok1 and ok2)
        except Exception as e:
            self.log.emit(f"[ERROR] {e}")
            self.finished.emit(False)


class InfoCard(QFrame):
    def __init__(self, title, value, color="#818CF8", parent=None):
        super().__init__(parent)
        self.setObjectName("StatCard") # Use same style as dashboard cards
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(4)

        lbl = QLabel(title)
        lbl.setObjectName("StatCardTitle")
        layout.addWidget(lbl)

        self.val = QLabel(str(value))
        self._color = color
        self.val.setStyleSheet(f"font-size: 22px; font-weight: 800; color: {color};")
        layout.addWidget(self.val)

    def set_value(self, value, color=None):
        self.val.setText(str(value))
        if color:
            self.val.setStyleSheet(f"color: {color};")


class SyncView(QWidget):
    data_loaded = Signal()

    def __init__(self, config, engine, parent=None):
        super().__init__(parent)
        self.setObjectName("SyncView")
        self.config = config
        self.engine = engine
        self.worker = None
        self._load_worker = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(32, 24, 32, 32)
        layout.setSpacing(16)

        from ui.components.icons import Icons
        header_layout = QHBoxLayout()
        header_layout.setSpacing(12)
        
        icon_label = QLabel()
        icon_label.setPixmap(Icons.get_pixmap(Icons.SYNC, 24, "#818CF8" if getattr(self, "is_dark", True) else "#4F46E5"))
        header_layout.addWidget(icon_label)
        
        header = QLabel("Synchronization")
        header.setObjectName("PageHeader")
        header_layout.addWidget(header)
        header_layout.addStretch() # Consistency
        layout.addLayout(header_layout)

        # Info Cards
        cards_layout = QGridLayout()
        cards_layout.setSpacing(12)
        self.card_rows = InfoCard("Ranking Rows", "0")
        self.card_dd_rows = InfoCard("Drill-down Rows", "0", "#EC4899")
        self.card_clients = InfoCard("Clients", "0", "#10B981")
        self.card_parquet = InfoCard("Ranking (Parquet)", "0 MB", "#F59E0B")
        self.card_dd_parquet = InfoCard("Drill-down (Parquet)", "0 MB", "#F59E0B")
        self.card_zip = InfoCard("ZIP Status", "---", "#94A3B8")
        
        cards_layout.addWidget(self.card_rows, 0, 0)
        cards_layout.addWidget(self.card_dd_rows, 0, 1)
        cards_layout.addWidget(self.card_clients, 0, 2)
        cards_layout.addWidget(self.card_parquet, 1, 0)
        cards_layout.addWidget(self.card_dd_parquet, 1, 1)
        cards_layout.addWidget(self.card_zip, 1, 2)
        layout.addLayout(cards_layout)

        # Load Section
        load_section = QFrame()
        load_section.setObjectName("FilterFrame") # Use FilterFrame style
        load_layout = QVBoxLayout(load_section)
        load_layout.setContentsMargins(20, 16, 20, 16)
        load_layout.setSpacing(12)

        load_title = QLabel("Initial Data Ingestion")
        load_title.setObjectName("DetailSubHeader")
        load_layout.addWidget(load_title)

        load_btns = QHBoxLayout()

        self.load_zip_btn = QPushButton(" Load ZIP")
        self.load_zip_btn.setIcon(Icons.get_icon(Icons.DOWNLOAD, 16, "white"))
        self.load_zip_btn.setObjectName("SuccessBtn")
        self.load_zip_btn.clicked.connect(self.load_zip)
        load_btns.addWidget(self.load_zip_btn)

        self.load_csv_btn = QPushButton(" Load CSV")
        self.load_csv_btn.setIcon(Icons.get_icon(Icons.DOWNLOAD, 16, "white"))
        self.load_csv_btn.setObjectName("InfoBtn")
        self.load_csv_btn.clicked.connect(self.load_csv)
        load_btns.addWidget(self.load_csv_btn)

        self.reload_btn = QPushButton(" Reload Local")
        self.reload_btn.setIcon(Icons.get_icon(Icons.SYNC, 16, "white"))
        self.reload_btn.setObjectName("ActionBtn")
        self.reload_btn.clicked.connect(self.reload_current)
        load_btns.addWidget(self.reload_btn)

        load_btns.addStretch()
        load_layout.addLayout(load_btns)
        layout.addWidget(load_section)

        # Status
        self.status_label = QLabel("Ready.")
        self.status_label.setObjectName("PageSubtitle")
        layout.addWidget(self.status_label)

        # Progress
        self.pbar = QProgressBar()
        self.pbar.setObjectName("LoadingBar")
        self.pbar.setFormat("%v / %m  (%p%)")
        layout.addWidget(self.pbar)

        # Log
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setObjectName("LogOutput")
        layout.addWidget(self.log_output)

        # Action Buttons
        btn_layout = QHBoxLayout()

        self.sync_btn = QPushButton(" Start Sync")
        self.sync_btn.setIcon(Icons.get_icon(Icons.SYNC, 16, "white"))
        self.sync_btn.setObjectName("PrimaryBtn")
        self.sync_btn.clicked.connect(self.start_sync)
        btn_layout.addWidget(self.sync_btn)

        from PySide6.QtWidgets import QComboBox
        
        self.type_combo = QComboBox()
        self.type_combo.addItem("➕ 差分更新 (マージ)", "diff")
        self.type_combo.addItem("🗑 全件再取得 (フルリセット)", "clean")
        self.type_combo.setFixedWidth(160)
        btn_layout.addWidget(self.type_combo)

        self.target_combo = QComboBox()
        self.target_combo.addItem("📦 全種データ", "full")
        self.target_combo.addItem("📥 ドリルダウンのみ", "drilldown")
        self.target_combo.addItem("👥 ランキングのみ", "client")
        self.target_combo.setFixedWidth(140)
        btn_layout.addWidget(self.target_combo)

        self.stop_btn = QPushButton("⏹  Stop")
        self.stop_btn.setObjectName("DangerBtn")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.stop_sync)
        btn_layout.addWidget(self.stop_btn)

        self.reset_btn = QPushButton("🗑  Reset Data")
        self.reset_btn.setObjectName("OutlineDangerBtn")
        self.reset_btn.clicked.connect(self.reset_data)
        btn_layout.addWidget(self.reset_btn)

        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        self.refresh_cards()

    def set_theme(self, is_dark):
        bg = "#F8FAFC" if not is_dark else "#0F172A"
        self.setStyleSheet(f"QWidget#SyncView {{ background-color: {bg}; }}")

    def refresh_cards(self):
        stats = self.engine.get_stats_summary()
        self.card_rows.set_value(f"{stats['total_rows']:,}")
        self.card_dd_rows.set_value(f"{stats.get('drilldown_rows', 0):,}")
        self.card_clients.set_value(f"{stats['total_clients']:,}")
        self.card_parquet.set_value(f"{stats.get('parquet_size_mb', 0)} MB")
        self.card_dd_parquet.set_value(f"{stats.get('drilldown_parquet_size_mb', 0)} MB")

        zip_path = os.path.join(self.engine.base_dir, 'streamdbi_data.zip')
        if os.path.exists(zip_path):
            size = round(os.path.getsize(zip_path) / 1048576, 1)
            self.card_zip.set_value(f"{size} MB", "#10B981")
        else:
            self.card_zip.set_value("Not Found", "#EF4444")

    def refresh(self):
        self.refresh_cards()

    def _set_load_buttons_enabled(self, enabled):
        self.load_zip_btn.setEnabled(enabled)
        self.load_csv_btn.setEnabled(enabled)
        self.reload_btn.setEnabled(enabled)

    # ── Load (background) ──
    def load_zip(self):
        default_zip = os.path.join(self.engine.base_dir, 'streamdbi_data.zip')
        if os.path.exists(default_zip):
            path = default_zip
        else:
            path, _ = QFileDialog.getOpenFileName(self, "ZIPファイルを選択", self.engine.base_dir, "ZIP files (*.zip)")
            if not path:
                return

        self._set_load_buttons_enabled(False)
        self.status_label.setText("Loading ZIP...")
        self._load_worker = DataLoadWorker(self.engine, "zip", path)
        self._load_worker.log.connect(self.append_log)
        self._load_worker.finished.connect(self._on_load_done)
        self._load_worker.start()

    def load_csv(self):
        path, _ = QFileDialog.getOpenFileName(self, "CSVファイルを選択", self.engine.base_dir, "CSV files (*.csv)")
        if not path:
            return

        self._set_load_buttons_enabled(False)
        self.status_label.setText("Loading CSV...")
        self._load_worker = DataLoadWorker(self.engine, "csv", path)
        self._load_worker.log.connect(self.append_log)
        self._load_worker.finished.connect(self._on_load_done)
        self._load_worker.start()

    def reload_current(self):
        self._set_load_buttons_enabled(False)
        self.status_label.setText("Reloading...")
        self._load_worker = DataLoadWorker(self.engine, "reload")
        self._load_worker.log.connect(self.append_log)
        self._load_worker.finished.connect(self._on_load_done)
        self._load_worker.start()

    def _on_load_done(self, success):
        self._set_load_buttons_enabled(True)
        self.status_label.setText("Load complete." if success else "Load failed.")
        self.refresh_cards()
        self.data_loaded.emit()

    # ── Sync ──
    def start_sync(self):
        self.config.reload()
        if not self.config.get("url") or not self.config.get("key"):
            self.append_log("[ERROR] URL or API Key is missing. Check Settings.")
            return

        self.sync_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.log_output.clear()
        self.status_label.setText("Syncing...")

        target = self.target_combo.currentData()
        sync_type = self.type_combo.currentData()
        self.worker = SyncWorker(self.config, self.engine, target=target, sync_type=sync_type)
        self.worker.progress.connect(self.update_progress)
        self.worker.rows_updated.connect(self.update_live_stats)
        self.worker.log.connect(self.append_log)
        self.worker.error.connect(self.on_error)
        self.worker.finished.connect(self.on_finished)
        self.worker.start()

    def stop_sync(self):
        if self.worker and self.worker.isRunning():
            self.worker.stop()
            self.append_log("[INFO] Stop requested...")
            self.status_label.setText("Stopping...")
            self.stop_btn.setEnabled(False)

    def update_progress(self, done, total):
        self.pbar.setMaximum(total)
        self.pbar.setValue(done)
        self.status_label.setText(f"Syncing... ({done:,}/{total:,})")

    def update_live_stats(self, ranking_rows, dd_rows):
        self.card_rows.set_value(f"{ranking_rows:,}")
        self.card_dd_rows.set_value(f"{dd_rows:,}")

    def append_log(self, text):
        self.log_output.append(text)
        
        # UI LAG PREVENTION: Limit buffer to 1000 lines
        doc = self.log_output.document()
        if doc.blockCount() > 1000:
            cursor = self.log_output.textCursor()
            cursor.movePosition(QTextCursor.Start)
            cursor.movePosition(QTextCursor.Down, QTextCursor.KeepAnchor, 200) # Remove top 200 lines
            cursor.removeSelectedText()
            
        sb = self.log_output.verticalScrollBar()
        sb.setValue(sb.maximum())

    def on_error(self, err_msg):
        self.append_log(f"\n[ERROR] {err_msg}")
        self.status_label.setText("Sync failed.")

    def on_finished(self):
        self.sync_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        if "Stopping" in self.status_label.text():
            self.status_label.setText("Sync stopped.")
        elif "failed" not in self.status_label.text().lower():
            self.status_label.setText("Sync finished.")
        self.refresh_cards()
        self.data_loaded.emit()

    def reset_data(self):
        self.engine.reset_data()
        self.append_log("[INFO] All local data reset.")
        self.pbar.setValue(0)
        self.refresh_cards()
