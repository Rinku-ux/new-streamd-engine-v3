from PySide6.QtWidgets import (QWidget, QVBoxLayout, QTableView, QLabel,
                               QHBoxLayout, QHeaderView, QLineEdit, QPushButton,
                               QAbstractItemView, QFrame, QScrollArea, QComboBox,
                               QFileDialog, QMessageBox, QDialog, QFormLayout, QCheckBox)
from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex, QTimer, QSize, Signal, QThread, Slot
from PySide6.QtGui import QColor
from ui.widgets.modern_progress import ModernProgressOverlay
import pandas as pd
import os
import subprocess
import shutil
import tempfile


class PandasModel(QAbstractTableModel):
    """High-performance table model backed by a pandas DataFrame."""

    def __init__(self, df=None):
        super().__init__()
        self._df = df if df is not None else pd.DataFrame()

    def set_dataframe(self, df):
        self.beginResetModel()
        # Pre-convert to string and handle NaN for performance during scrolling
        self._df = df.copy().astype(str).replace(['nan', 'None', '<NA>'], '')
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QModelIndex()):
        return len(self._df.columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.DisplayRole:
            return self._df.iloc[index.row(), index.column()]
        if role == Qt.TextAlignmentRole:
            # Right-align numeric columns if needed, but for now keep left
            return Qt.AlignLeft | Qt.AlignVCenter
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                return str(self._df.columns[section])
            return str(section + 1)
        return None


class GlobalDrilldownDialog(QDialog):
    """Dialog for exporting drill-down data across all clients."""
    
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.setWindowTitle("📥 全クライアント一括ドリルダウン出力")
        self.setMinimumWidth(380)
        self.engine = engine
        
        layout = QVBoxLayout(self)
        
        info = QLabel("全てのクライアントから条件に合致する修正内容をまとめてCSV出力します。")
        info.setWordWrap(True)
        info.setStyleSheet("color: #4F46E5; font-weight: bold; margin-bottom: 8px;")
        layout.addWidget(info)
        
        form = QFormLayout()
        
        self.cb_month = QComboBox()
        self.cb_month.addItem("すべて", "")
        # MODIFIED: Only show months that actually exist in drilldown_data
        try:
            months = self.engine.query('SELECT DISTINCT "target_month" FROM drilldown_data WHERE "target_month" IS NOT NULL ORDER BY "target_month" DESC')
            for row in months:
                month = str(row.get("target_month"))
                self.cb_month.addItem(month, month)
        except:
            # Fallback to master_data months if drilldown_data table is empty or missing
            for row in self.engine.query('SELECT DISTINCT "処理月" FROM master_data WHERE "処理月" IS NOT NULL ORDER BY "処理月" DESC'):
                month = str(row.get("処理月"))
                self.cb_month.addItem(month, month)
            
        self.cb_vtype = QComboBox()
        self.cb_vtype.addItem("すべて", "")
        for row in self.engine.query('SELECT DISTINCT "証憑タイプ" FROM master_data WHERE "証憑タイプ" IS NOT NULL ORDER BY "証憑タイプ"'):
            vt = str(row.get("証憑タイプ"))
            self.cb_vtype.addItem(vt, vt)
            
        self.cb_field = QComboBox()
        self.cb_field.addItem("すべて", "")
        # Get unique error fields from drilldown if index logic exists (or just standard values)
        try:
            for row in self.engine.query('SELECT DISTINCT "error_field" FROM drilldown_data ORDER BY "error_field"'):
                # Note: error_field often looks like date_target, amount_target etc.
                ef = str(row.get("error_field"))
                self.cb_field.addItem(ef, ef)
        except:
            pass  # If drilldown not loaded yet
            
        form.addRow("対象月:", self.cb_month)
        form.addRow("証憑タイプ:", self.cb_vtype)
        form.addRow("エラー項目:", self.cb_field)
        
        self.chk_codemap = QCheckBox("コードを日本語に翻訳して出力する (コードマップ反映)")
        self.chk_codemap.setChecked(True)
        form.addRow("", self.chk_codemap)
        
        layout.addLayout(form)
        
        btn_layout = QHBoxLayout()
        btn_cancel = QPushButton("キャンセル")
        btn_cancel.clicked.connect(self.reject)
        
        self.btn_export = QPushButton("CSV出力")
        self.btn_export.setObjectName("PrimaryBtn")
        self.btn_export.clicked.connect(self.accept)
        
        btn_layout.addStretch()
        btn_layout.addWidget(btn_cancel)
        btn_layout.addWidget(self.btn_export)
        layout.addLayout(btn_layout)


class GlobalExportWorker(QThread):
    finished = Signal(bool, str) # (success, message)
    progress = Signal(str)

    def __init__(self, engine, sql, output_path, use_codemap, config):
        super().__init__()
        import subprocess
        self.engine = engine
        self.sql = sql
        self.output_path = output_path
        self.use_codemap = use_codemap
        self.config = config
        self.is_main_table = False 
        self._r_path = self._find_r()

    def _find_r(self):
        """Check if Rscript is available in common locations."""
        import shutil
        import os
        r = shutil.which("Rscript")
        if r: return r
        
        # Check common Windows paths
        paths = [
            r"C:\Program Files\R\*\bin\Rscript.exe",
            r"C:\Program Files (x86)\R\*\bin\Rscript.exe"
        ]
        import glob
        for p in paths:
            found = glob.glob(p)
            if found:
                found.sort(reverse=True)
                # Prefer x64 if it exists
                x64_path = os.path.join(os.path.dirname(found[0]), 'x64', 'Rscript.exe')
                if os.path.exists(x64_path):
                    return x64_path
                return found[0]
        return None

    def _get_short_path(self, path):
        """Get 8.3 short path for Windows to avoid non-ASCII issues."""
        import ctypes
        if not path: return path
        try:
            buf = ctypes.create_unicode_buffer(1024)
            ctypes.windll.kernel32.GetShortPathNameW(path, buf, 1024)
            return buf.value or path
        except:
            return path

    def run(self):
        try:
            self.progress.emit("データを抽出中...")
            # For small-medium exports, query_df + translation is fine.
            # For massive ones, DuckDB COPY TO is better, but we need codemap integration.
            
            df = self.engine.query_df(self.sql)
            if df is None or df.empty:
                self.finished.emit(False, "該当するデータが見つかりませんでした。")
                return

            if self.use_codemap and not self._r_path:
                self.progress.emit("コードマップを適用中 (Python)...")
                # We reuse the logic from the view but in this thread
                # This is only a fallback, it's slow!
                code_map = self.config.get("code_map", {})
                account_map = code_map.get("account", {})
                tax_map = code_map.get("tax", {})

                def map_value(row, col):
                    val = str(row.get(col, ""))
                    error_field = str(row.get('エラー項目', ''))
                    if not val or val == "nan": return val
                    if "科目" in error_field or "account" in error_field:
                        return ", ".join(account_map.get(v.strip(), v.strip()) for v in val.split(","))
                    elif "税区分" in error_field or "tax" in error_field:
                        return ", ".join(tax_map.get(v.strip(), v.strip()) for v in val.split(","))
                    return val

                if '初期値' in df.columns and '修正後' in df.columns:
                    df['初期値'] = df.apply(lambda row: map_value(row, '初期値'), axis=1)
                    df['修正後'] = df.apply(lambda row: map_value(row, '修正後'), axis=1)

            # ISOALTION STRATEGY: Copy everything to a clean temp directory
            if self._r_path:
                self.progress.emit("R (data.table) を使用してエクスポート準備中...")
                
                # Create a temporary workspace (tempfile usually handles ASCII-only paths on Windows defaults)
                with tempfile.TemporaryDirectory() as tmp_dir:
                    # Path resolution from ui/views/ranking.py -> root -> core/fast_export.R
                    app_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                    r_script_src = os.path.abspath(os.path.join(app_root, 'core', 'fast_export.R'))
                    r_script_tmp = os.path.join(tmp_dir, "export.R")
                    shutil.copy2(r_script_src, r_script_tmp)
                    
                    # 2. Write data to temp workspace
                    data_tmp = os.path.join(tmp_dir, "data.csv")
                    df.to_csv(data_tmp, index=False, encoding='utf-8')
                    
                    # 3. Handle config if needed
                    config_tmp = None
                    if self.use_codemap:
                        config_tmp = os.path.join(tmp_dir, "config.json")
                        import json
                        with open(config_tmp, 'w', encoding='utf-8') as f:
                            json.dump(self.config.data, f, ensure_ascii=False)
                    
                    # REFRESH PATH: Ensure we don't use a stale path if R was updated
                    self._r_path = self._find_r()
                    if not self._r_path:
                        self.finished.emit(False, "R言語が見つかりません。")
                        return

                    # ISOALTION STRATEGY: Copy everything to a clean temp directory
                    # We use long path for R executable but short paths for everything else
                    self.progress.emit(f"Rを実行中 (R: {os.path.basename(self._r_path)})...")
                    
                    final_data_tmp = os.path.join(tmp_dir, "output.csv")
                    
                    # Use shell=True and manual quoting for maximum Windows compatibility
                    cmd = f'"{self._r_path}" --vanilla "{r_script_tmp}" "{data_tmp}" "{final_data_tmp}"'
                    if self.use_codemap:
                        cmd += f' "{config_tmp}"'
                    
                    try:
                        res = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', cwd=tmp_dir, shell=True)
                        
                        # HARDENED SUCCESS CHECK: Look at both stdout and stderr
                        if "DONE_SUCCESS" in res.stdout or "DONE_SUCCESS" in res.stderr:
                            # Copy the final result from the safe temp path to the user's requested path
                            if os.path.exists(final_data_tmp):
                                if os.path.exists(self.output_path): os.remove(self.output_path)
                                shutil.copy2(final_data_tmp, self.output_path)
                                self.finished.emit(True, f"R(data.table)を使用してエクスポートが完了しました:\n{self.output_path}")
                                return
                            else:
                                raise FileNotFoundError("Rは成功しましたが、出力ファイルが見当たりません。")
                        else:
                            err_msg = f"R Error (Exit Code {res.returncode}):\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}"
                            print(f"[R_ERR] {err_msg}")
                            # FALLBACK
                            self.progress.emit("R実行に失敗したため、標準エンジンで書き出し中...")
                            df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
                            self.finished.emit(True, f"エクスポートが完了しました (Rエラーのため標準エンジンを使用):\n{self.output_path}\n\nRのエラー内容:\n{err_msg}")
                            return
                    except Exception as sub_e:
                        print(f"[R_CRITICAL] {sub_e}")
                        # FALLBACK
                        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
                        self.finished.emit(True, f"エクスポートが完了しました (R起動失敗のため標準エンジンを使用):\n{self.output_path}")
                        return
            else:
                self.finished.emit(False, "R言語(Rscript.exe)が見つかりません。パスが通っているか、設定を確認してください。")
                return

        except Exception as e:
            self.finished.emit(False, f"エクスポートエラー: {e}")

class RankingView(QWidget):
    client_selected = Signal(str, str)  # (client_id, enterprise_name)

    def __init__(self, config, engine, parent=None):
        super().__init__(parent)
        self.setObjectName("RankingView")
        self.config = config
        self.engine = engine
        self._model = PandasModel()
        self._detail_model = PandasModel()
        self._drilldown_model = PandasModel()
        self.current_page = 1
        self.per_page = 100
        self.total_count = 0
        self.mode = "summary" # "summary", "detail", or "drilldown"
        self.current_client_id = None
        self._export_worker = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(32, 24, 32, 32)
        layout.setSpacing(16)

        # Header
        header_layout = QHBoxLayout()
        header_layout.setSpacing(12)
        
        from ui.components.icons import Icons
        icon_label = QLabel()
        icon_label.setPixmap(Icons.get_pixmap(Icons.TABLE, 24, "#818CF8" if getattr(self, "_is_dark", True) else "#4F46E5"))
        header_layout.addWidget(icon_label)
        
        header = QLabel("データテーブル")
        header.setObjectName("PageHeader")
        header_layout.addWidget(header)

        self.row_count_label = QLabel("")
        self.row_count_label.setObjectName("PageSubtitle")
        header_layout.addWidget(self.row_count_label)
        header_layout.addStretch()

        # Mode Selection
        mode_layout = QHBoxLayout()
        mode_layout.setSpacing(0)
        self.btn_summary = QPushButton("企業別集計")
        self.btn_detail = QPushButton("詳細 (月別)")
        self.btn_drilldown = QPushButton("ドリルダウン (全件)")
        
        self.btn_summary.setObjectName("ModeBtnLeft")
        self.btn_detail.setObjectName("ModeBtnCenter")
        self.btn_drilldown.setObjectName("ModeBtnRight")
        
        self.btn_summary.clicked.connect(lambda: self.set_mode("summary"))
        self.btn_detail.clicked.connect(lambda: self.set_mode("detail"))
        self.btn_drilldown.clicked.connect(lambda: self.set_mode("drilldown"))
        
        mode_layout.addWidget(self.btn_summary)
        mode_layout.addWidget(self.btn_detail)
        mode_layout.addWidget(self.btn_drilldown)
        header_layout.addLayout(mode_layout)

        # CSV Export Button
        btn_export_layout = QHBoxLayout()
        self.export_btn = QPushButton(" 一覧をCSVエクスポート")
        self.export_btn.setIcon(Icons.get_icon(Icons.DOWNLOAD, 16, "white"))
        self.export_btn.setStyleSheet("""
            QPushButton { background-color: #059669; color: white; padding: 6px 14px;
                         border-radius: 6px; font-weight: 700; font-size: 11px; }
            QPushButton:hover { background-color: #047857; }
        """)
        self.export_btn.setCursor(Qt.PointingHandCursor)
        self.export_btn.clicked.connect(self.export_csv)
        
        self.global_dd_btn = QPushButton(" まとめてドリルダウン出力")
        self.global_dd_btn.setIcon(Icons.get_icon(Icons.DOWNLOAD, 16, "white"))
        self.global_dd_btn.setStyleSheet("""
            QPushButton { background-color: #4F46E5; color: white; padding: 6px 14px;
                         border-radius: 6px; font-weight: 700; font-size: 11px; }
            QPushButton:hover { background-color: #4338CA; }
        """)
        self.global_dd_btn.setCursor(Qt.PointingHandCursor)
        self.global_dd_btn.clicked.connect(self._show_global_drilldown)

        btn_export_layout.addWidget(self.export_btn)
        btn_export_layout.addWidget(self.global_dd_btn)
        header_layout.addLayout(btn_export_layout)
        
        layout.addLayout(header_layout)
        
        self.update_mode_ui()
        
        # Modern Progress Overlay (init at end of constructor)
        self.progress_overlay = ModernProgressOverlay(self)

        search_layout = QHBoxLayout()
        search_layout.setSpacing(10)
        search_label = QLabel()
        search_label.setPixmap(Icons.get_pixmap(Icons.SEARCH, 18, "#94A3B8"))
        search_layout.addWidget(search_label)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText('SQLフィルタ (例: WHERE "クライアントID" = \'412070\')')
        self.search_input.returnPressed.connect(self.refresh)

        
        # New: SQL Template Dropdown
        self.template_combo = QComboBox()
        self.sql_templates = {
            "SQLテンプレートを選択...": "",
            "全件表示 (クリア)": "",
            "クライアントIDで絞り込む": 'WHERE "クライアントID" = \'\'',
            "企業名で検索 (部分一致)": 'WHERE "企業名" LIKE \'%%\'',
                "処理月で絞り込む": 'WHERE "処理月" = \'2024-03\'',
            "証憑タイプで絞り込む": 'WHERE "証憑タイプ" = \'receipt\'',
            "正解件数が10件以上のもの": 'WHERE CAST("全体正解件数" AS INTEGER) >= 10',
            "正解率が低い順に並べ替え": 'ORDER BY "正解率%" ASC'
        }
        self.template_combo.addItems(self.sql_templates.keys())
        self.template_combo.activated.connect(self._on_template_selected)
        
        search_layout.addWidget(self.template_combo)
        search_layout.addWidget(self.search_input)

        search_btn = QPushButton("検索")
        search_btn.setStyleSheet("""
            QPushButton { background-color: #4F46E5; color: white; padding: 10px 20px;
                         border-radius: 8px; font-weight: 700; }
            QPushButton:hover { background-color: #4338CA; }
        """)
        search_btn.clicked.connect(self.refresh)
        search_layout.addWidget(search_btn)


        # QTableView
        self.table = QTableView()
        self.table.setModel(self._model)
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setWordWrap(False)
        self.table.horizontalHeader().setDefaultSectionSize(120)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.verticalHeader().setDefaultSectionSize(28)
        self.table.selectionModel().selectionChanged.connect(self._on_selection_changed)
        
        # Split Layout Container
        self.main_split_layout = QHBoxLayout()
        self.main_split_layout.setSpacing(16)
        
        # Left Side (Master)
        master_widget = QWidget()
        master_widget.setObjectName("RankingContainer")
        master_vbox = QVBoxLayout(master_widget)
        master_vbox.setContentsMargins(0,0,0,0)
        master_vbox.addLayout(search_layout)
        master_vbox.addWidget(self.table)
        
        # Pagination Bar (Inside Master)
        page_layout = QHBoxLayout()
        page_layout.setContentsMargins(0, 8, 0, 0)
        # ... (rest of pagination buttons)
        page_layout.addStretch()
        
        self.btn_prev = QPushButton("← 前へ")
        self.btn_next = QPushButton("次へ →")
        self.page_label = QLabel("Page 1")
        self.page_label.setStyleSheet("color: #94A3B8; font-size: 13px; font-weight: 600; margin: 0 16px;")
        
        btn_page_style = """
            QPushButton { border-radius: 6px; padding: 6px 12px; font-weight: 600; }
        """
        self.btn_prev.setStyleSheet(btn_page_style)
        self.btn_next.setStyleSheet(btn_page_style)
        self.btn_prev.clicked.connect(self.prev_page)
        self.btn_next.clicked.connect(self.next_page)
        
        page_layout.addWidget(self.btn_prev)
        page_layout.addWidget(self.page_label)
        page_layout.addWidget(self.btn_next)
        page_layout.addStretch()
        master_vbox.addLayout(page_layout)
        
        self.main_split_layout.addWidget(master_widget, 2) # Ratio 2
        
        # Right Side (Detail Panel)
        self.detail_panel = QFrame()
        self.detail_panel.setObjectName("DetailPanel")
        self.detail_panel.setMinimumWidth(400)
        detail_layout = QVBoxLayout(self.detail_panel)
        detail_layout.setContentsMargins(20, 20, 20, 20)
        
        self.detail_title = QLabel("企業を選択してください")
        self.detail_title.setObjectName("DetailTitle")
        self.detail_title.setWordWrap(True)
        detail_layout.addWidget(self.detail_title)
        
        self.detail_subtitle = QLabel("左のテーブルから行を選択すると詳細が表示されます")
        self.detail_subtitle.setObjectName("DetailSubtitle")
        detail_layout.addWidget(self.detail_subtitle)
        
        # Detail Stats
        self.detail_stats = QLabel("")
        self.detail_stats.setStyleSheet("color: #10B981; font-weight: 700; margin-top: 10px;")
        detail_layout.addWidget(self.detail_stats)

        # Button to view trend on dashboard
        self.trend_btn = QPushButton("📊  ダッシュボードで推移を見る")
        self.trend_btn.setStyleSheet("""
            QPushButton { background-color: #4F46E5; color: white; padding: 8px 16px;
                         border-radius: 6px; font-weight: 700; font-size: 11px; }
            QPushButton:hover { background-color: #4338CA; }
        """)
        self.trend_btn.setCursor(Qt.PointingHandCursor)
        self.trend_btn.clicked.connect(self._emit_client_selected)
        self.trend_btn.setVisible(False)
        detail_layout.addWidget(self.trend_btn)
        
        detail_sub_header = QLabel("📊 月別詳細推移")
        detail_sub_header.setObjectName("DetailSubHeader")
        detail_layout.addWidget(detail_sub_header)
        
        self.detail_table = QTableView()
        self.detail_table.setModel(self._detail_model)
        self.detail_table.horizontalHeader().setStretchLastSection(True)
        self.detail_table.clicked.connect(self._on_detail_row_clicked)
        detail_layout.addWidget(self.detail_table)
        
        # New: Drill-down Error Table
        dd_header = QLabel("🔍 ドリルダウン詳細 (修正箇所)")
        dd_header.setObjectName("DetailSubHeader")
        detail_layout.addWidget(dd_header)
        
        self.drilldown_table = QTableView()
        self.drilldown_table.setModel(self._drilldown_model)
        self.drilldown_table.verticalHeader().setVisible(False)
        self.drilldown_table.horizontalHeader().setStretchLastSection(True)
        detail_layout.addWidget(self.drilldown_table)
        
        self.main_split_layout.addWidget(self.detail_panel, 1) # Ratio 1
        
        layout.addLayout(self.main_split_layout)

    def _translate_drilldown(self, df):
        """Translates numerical codes in '初期値' and '修正後' according to code_map."""
        code_map = self.config.get("code_map", {})
        account_map = code_map.get("account", {})
        tax_map = code_map.get("tax", {})

        def map_value(row, col):
            val = str(row.get(col, ""))
            error_field = str(row.get('項目', row.get('エラー項目', '')))
            
            if not val or val == "nan":
                return val
                
            if "科目" in error_field or "account" in error_field:
                return ", ".join(account_map.get(v.strip(), v.strip()) for v in val.split(","))
            elif "税区分" in error_field or "tax" in error_field:
                return ", ".join(tax_map.get(v.strip(), v.strip()) for v in val.split(","))
            
            return val
            
        if not df.empty and '初期値' in df.columns and '修正後' in df.columns:
            df['初期値'] = df.apply(lambda row: map_value(row, '初期値'), axis=1)
            df['修正後'] = df.apply(lambda row: map_value(row, '修正後'), axis=1)
            
        return df

    def set_mode(self, mode):
        self.mode = mode
        self.current_page = 1
        self.update_mode_ui()
        self.refresh()

    def set_theme(self, is_dark):
        self._is_dark = is_dark
        self.update_mode_ui()
        
        # Ensure self background is correct (reinforce QSS)
        bg = "#F8FAFC" if not is_dark else "#0F172A"
        self.setStyleSheet(f"QWidget#RankingView {{ background-color: {bg}; }}")

    def update_mode_ui(self):
        is_dark = getattr(self, "_is_dark", True)
        
        # Consistent colors with the rest of the app
        active_bg = "#4F46E5"
        active_fg = "#FFFFFF"
        
        inactive_bg = "#1E293B" if is_dark else "#F1F5F9"
        inactive_fg = "#94A3B8" if is_dark else "#64748B"
        inactive_border = "#334155" if is_dark else "#E2E8F0"

        active_s = f"background-color: {active_bg}; color: {active_fg}; border-color: {active_bg};"
        inactive_s = f"background-color: {inactive_bg}; color: {inactive_fg}; border-color: {inactive_border};"

        common_style = "padding: 6px 16px; font-size: 11px; font-weight: 700;"
        
        self.btn_summary.setStyleSheet(f"QPushButton {{ {common_style} border-top-left-radius: 6px; border-bottom-left-radius: 6px; {active_s if self.mode == 'summary' else inactive_s} }}")
        self.btn_detail.setStyleSheet(f"QPushButton {{ {common_style} border-left: none; {active_s if self.mode == 'detail' else inactive_s} }}")
        self.btn_drilldown.setStyleSheet(f"QPushButton {{ {common_style} border-top-right-radius: 6px; border-bottom-right-radius: 6px; border-left: none; {active_s if self.mode == 'drilldown' else inactive_s} }}")

    def prev_page(self):
        if self.current_page > 1:
            self.current_page -= 1
            self.refresh()

    def next_page(self):
        if len(self._model._df) == self.per_page:
            self.current_page += 1
            self.refresh()

    def refresh(self):
        """Runs query on main thread (DuckDB not thread-safe), deferred via QTimer."""
        if not self.engine.has_data():
            self.row_count_label.setText("No data loaded.")
            self._model.set_dataframe(pd.DataFrame())
            return

        self.row_count_label.setText("Loading...")
        QTimer.singleShot(10, self._do_query)

    def _do_query(self):
        where_clause = self.search_input.text().strip()
        if where_clause.endswith(';'):
            where_clause = where_clause[:-1].strip()
        if where_clause:
            upper_sql = where_clause.upper()
            if not any(upper_sql.startswith(k) for k in ["WHERE", "ORDER", "GROUP", "LIMIT"]):
                where_clause = f"WHERE {where_clause}"

        offset = (self.current_page - 1) * self.per_page
        
        if self.mode == "summary":
            sql = f"""
                SELECT 
                    "クライアントID", "企業名", 
                    COUNT("処理月") as "月数",
                    SUM(CAST("対象仕訳数" AS INTEGER)) as "対象仕訳数_計",
                    SUM(CAST("全体正解件数" AS INTEGER)) as "全体正解件数_計"
                FROM master_data 
                {where_clause}
                GROUP BY "クライアントID", "企業名"
                ORDER BY "対象仕訳数_計" DESC
                LIMIT {self.per_page} OFFSET {offset}
            """
        elif self.mode == "detail":
            sql = f"SELECT * FROM master_data {where_clause} ORDER BY \"企業名\", \"処理月\" DESC LIMIT {self.per_page} OFFSET {offset}"
        else: # drilldown
            # Map where clause fields if needed
            dd_where = where_clause.replace("\"クライアントID\"", "\"client_id\"").replace("\"処理月\"", "\"target_month\"").replace("\"証憑タイプ\"", "\"voucher_type\"")
            sql = f'SELECT * FROM drilldown_data {dd_where} ORDER BY "target_month" DESC LIMIT {self.per_page} OFFSET {offset}'

        try:
            df = self.engine.query_df(sql)
            
            # Update UI state
            self.page_label.setText(f"Page {self.current_page}")
            self.btn_prev.setEnabled(self.current_page > 1)
            self.btn_next.setEnabled(len(df) == self.per_page)
            
        except Exception as e:
            self.row_count_label.setText(f"Query Error: {e}")
            return

        if df is None or df.empty:
            self.row_count_label.setText("No results.")
            self._model.set_dataframe(pd.DataFrame())
            return

        total = self.engine.get_row_count()
        self.row_count_label.setText(f"Showing page {self.current_page} ({len(df):,} items)")
        self._model.set_dataframe(df)

    def _on_selection_changed(self, selected, deselected):
        indexes = self.table.selectionModel().selectedRows()
        if not indexes:
            return
            
        row = indexes[0].row()
        df = self._model._df
        
        # Get identifier (Client ID preferred, then name)
        client_id = str(df.iloc[row].get("クライアントID", ""))
        enterprise_name = str(df.iloc[row].get("企業名", ""))
        
        if not client_id and not enterprise_name:
            return
            
        self.current_client_id = client_id
        self.detail_title.setText(enterprise_name if enterprise_name else client_id)
        self.detail_subtitle.setText(f"ID: {client_id}" if client_id else "")
        self.trend_btn.setVisible(bool(client_id))
        
        # Build query for history
        if client_id:
            where = f"WHERE \"クライアントID\" = '{client_id}'"
        else:
            where = f"WHERE \"企業名\" = '{enterprise_name}'"
            
        sql = f"""
            SELECT "処理月", "証憑タイプ", "対象仕訳数", "全体正解件数",
                   round(CAST("全体正解件数" AS FLOAT) / NULLIF(CAST("対象仕訳数" AS INTEGER), 0) * 100, 1) as "正解率%"
            FROM master_data 
            {where}
            ORDER BY "処理月" DESC
        """
        
        try:
            detail_df = self.engine.query_df(sql)
            if detail_df is not None and not detail_df.empty:
                self._detail_model.set_dataframe(detail_df)
                self.detail_table.resizeColumnsToContents()
                
                # Summary for panel
                if "対象仕訳数" in detail_df.columns:
                    # Safe sum with numeric conversion
                    import pandas as pd
                    total_v = pd.to_numeric(detail_df["対象仕訳数"], errors='coerce').fillna(0).astype(int).sum()
                    avg_acc = detail_df["正解率%"].mean() if "正解率%" in detail_df.columns else 0
                    self.detail_stats.setText(f"📊 累計対象: {total_v:,}件  |  平均正解率: {avg_acc:.1f}%")
                else:
                    self.detail_stats.setText("📊 統計データ不備")
            else:
                self._detail_model.set_dataframe(pd.DataFrame(columns=["処理月", "証憑タイプ", "対象仕訳数", "全体正解件数"]))
                self.detail_stats.setText("📊 データが見つかりません")

            # Query Drill-down data (Q1011)
            dd_sql = f"""
                SELECT "target_month" as "月", "error_field" as "項目", "initial_value" as "初期値", "latest_value" as "修正後"
                FROM drilldown_data
                WHERE "client_id" = '{client_id}'
                ORDER BY "target_month" DESC
                LIMIT 500
            """
            try:
                dd_df = self.engine.query_df(dd_sql)
                if dd_df is not None:
                    dd_df = self._translate_drilldown(dd_df)
                    self._drilldown_model.set_dataframe(dd_df)
                    self.drilldown_table.resizeColumnsToContents()
            except:
                # Table might not exist yet if sync never finished
                self._drilldown_model.set_dataframe(pd.DataFrame(columns=["項目", "初期値", "修正後"]))

        except Exception as e:
            print(f"Detail query failed: {e}")

    def _on_template_selected(self, index):
        template_name = self.template_combo.itemText(index)
        sql = self.sql_templates.get(template_name, "")
        if template_name != "SQLテンプレートを選択...":
            self.search_input.setText(sql)
            self.search_input.setFocus()
            # If it's a filter with empty value, move cursor back
            if "''" in sql:
                self.search_input.setCursorPosition(len(sql) - 1)
            elif "%%" in sql:
                self.search_input.setCursorPosition(len(sql) - 2)

    def _on_detail_row_clicked(self, index):
        if not self.current_client_id:
            return
            
        row = index.row()
        df = self._detail_model._df
        month = str(df.iloc[row].get("処理月", ""))
        v_type = str(df.iloc[row].get("証憑タイプ", ""))
        
        where_clause = f"WHERE \"client_id\" = '{self.current_client_id}'"
        if month:
            where_clause += f" AND \"target_month\" = '{month}'"
        if v_type:
            where_clause += f" AND \"voucher_type\" = '{v_type}'"
            
        dd_sql = f"""
            SELECT "target_month" as "月", "error_field" as "項目", "initial_value" as "初期値", "latest_value" as "修正後"
            FROM drilldown_data
            {where_clause}
            ORDER BY "target_month" DESC
            LIMIT 500
        """
        try:
            dd_df = self.engine.query_df(dd_sql)
            if dd_df is not None:
                dd_df = self._translate_drilldown(dd_df)
                self._drilldown_model.set_dataframe(dd_df)
                self.drilldown_table.resizeColumnsToContents()
        except Exception as e:
            print(f"Filtered drill-down query failed: {e}")

    def _emit_client_selected(self):
        """Emit signal to navigate to dashboard with client trend."""
        if self.current_client_id:
            name = self.detail_title.text()
            self.client_selected.emit(self.current_client_id, name)

    def export_csv(self):
        """Export current view to CSV file via background thread."""
        path, _ = QFileDialog.getSaveFileName(
            self, "CSVエクスポート", "",
            "CSV files (*.csv);;All files (*)"
        )
        if not path:
            return
        
        try:
            where_clause = self.search_input.text().strip()
            if where_clause and not any(where_clause.upper().startswith(k) for k in ["WHERE", "ORDER", "GROUP", "LIMIT"]):
                where_clause = f"WHERE {where_clause}"
            
            if self.mode == "summary":
                sql = f"""
                    SELECT 
                        "クライアントID", "企業名", 
                        COUNT("処理月") as "月数",
                        SUM(CAST("対象仕訳数" AS INTEGER)) as "対象仕訳数_計",
                        SUM(CAST("全体正解件数" AS INTEGER)) as "全体正解件数_計"
                    FROM master_data 
                    {where_clause}
                    GROUP BY "クライアントID", "企業名"
                    ORDER BY "対象仕訳数_計" DESC
                """
            elif self.mode == "detail":
                sql = f'SELECT * FROM master_data {where_clause} ORDER BY "企業名", "処理月" DESC'
            else: # drilldown
                dd_where = where_clause.replace("\"クライアントID\"", "\"client_id\"").replace("\"処理月\"", "\"target_month\"").replace("\"証憑タイプ\"", "\"voucher_type\"")
                sql = f'SELECT * FROM drilldown_data {dd_where} ORDER BY "target_month" DESC'
            
            self.export_btn.setEnabled(False)
            self.progress_overlay.show_with_status("エクスポートデータを準備中...")
            
            self._export_worker = GlobalExportWorker(self.engine, sql, path, False, self.config)
            self._export_worker.is_main_table = True
            self._export_worker.progress.connect(self.progress_overlay.update_status)
            self._export_worker.finished.connect(self._on_main_export_finished)
            self._export_worker.start()

        except Exception as e:
            QMessageBox.warning(self, "エラー", f"エクスポートエラー: {e}")

    def _on_main_export_finished(self, success, message):
        self.export_btn.setEnabled(True)
        self.progress_overlay.hide()
        self.row_count_label.setText("Ready")
        if success:
            QMessageBox.information(self, "完了", message)
        else:
            QMessageBox.warning(self, "エラー", message)

    # ─────────────── Drilldown Export ───────────────

    def _show_global_drilldown(self):
        """Open dialog for global drilldown export."""
        dialog = GlobalDrilldownDialog(self.engine, self)
        if dialog.exec() == QDialog.Accepted:
            month = dialog.cb_month.currentData()
            vt    = dialog.cb_vtype.currentData()
            ef    = dialog.cb_field.currentData()
            use_codemap = dialog.chk_codemap.isChecked()
            self._export_global_drilldown(month, vt, ef, use_codemap)

    def _export_global_drilldown(self, target_month, voucher_type, error_field, use_codemap):
        """Builds JOIN query and exports via background thread to prevent UI freeze."""
        path, _ = QFileDialog.getSaveFileName(
            self, "全社ドリルダウン出力", "global_drilldown.csv",
            "CSV files (*.csv);;All files (*)"
        )
        if not path:
            return

        where_parts = []
        if target_month:
            where_parts.append(f"d.\"target_month\" = '{target_month}'")
        if voucher_type:
            where_parts.append(f"d.\"voucher_type\" = '{voucher_type}'")
        if error_field:
            where_parts.append(f"d.\"error_field\" = '{error_field}'")
            
        where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""

        sql = f"""
            SELECT 
                d."client_id" as "クライアントID",
                m."企業名",
                d."target_month" as "月",
                d."voucher_type" as "証憑タイプ",
                d."journal_id" as "仕訳ID",
                d."error_field" as "エラー項目",
                d."initial_value" as "初期値",
                d."latest_value" as "修正後"
            FROM drilldown_data d
            LEFT JOIN (
                SELECT DISTINCT "クライアントID", "企業名" FROM master_data
            ) m ON d."client_id" = m."クライアントID"
            {where_clause}
            ORDER BY d."target_month" DESC, d."client_id"
        """
        
        # Disable buttons during export
        self.global_dd_btn.setEnabled(False)
        self.progress_overlay.show_with_status("ドリルダウンデータを集計中...")

        self._export_worker = GlobalExportWorker(self.engine, sql, path, use_codemap, self.config)
        self._export_worker.progress.connect(self.progress_overlay.update_status)
        self._export_worker.finished.connect(self._on_export_finished)
        self._export_worker.start()

    def _on_export_finished(self, success, message):
        self.global_dd_btn.setEnabled(True)
        self.progress_overlay.hide()
        self.row_count_label.setText("Ready")
        
        if success:
            QMessageBox.information(self, "完了", message)
        else:
            QMessageBox.warning(self, "エラー", message)
