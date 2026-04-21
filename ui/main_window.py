from PySide6.QtWidgets import (QMainWindow, QWidget, QHBoxLayout, QStackedWidget,
                               QLabel, QVBoxLayout, QProgressBar, QToolTip, QInputDialog, QLineEdit, QMessageBox)
from PySide6.QtCore import Qt, QThread, Signal, QTimer, QFileSystemWatcher
from PySide6.QtGui import QFont
import os
import ctypes
from ui.components.sidebar import Sidebar

# ==== Light Theme QSS ====
LIGHT_THEME = """
QToolTip {
    background-color: #FFFFFF !important;
    color: #1E293B !important;
    border: 1px solid #CBD5E1 !important;
    border-radius: 2px;
    padding: 4px;
    font-family: 'Segoe UI';
    font-size: 12px;
}

QMainWindow { background-color: #F8FAFC; }
#CentralWidget { background-color: #F8FAFC; }
QStackedWidget { background-color: #F8FAFC; }
QWidget { font-family: 'Segoe UI', 'Noto Sans JP', 'Inter', sans-serif; color: #1E293B; }

/* Content Views */
#DashboardView, #RankingView, #SyncView, #SettingsView, #CodeMapView {
    background-color: #F8FAFC;
}

/* Scroll Areas & Containers */
QScrollArea, #DashboardScroll, #RankingScroll, #SyncScroll {
    background-color: transparent;
    border: none;
}
QScrollArea > QWidget > QWidget { /* Viewport inner widget */
    background-color: transparent;
}
#DashboardContainer, #RankingContainer, #SyncContainer {
    background-color: #F8FAFC;
}

/* Typography */
QLabel#PageHeader { font-size: 28px; font-weight: 800; color: #0F172A; }
QLabel#PageSubtitle { font-size: 14px; color: #64748B; }
QLabel#FilterLabel { color: #64748B; font-weight: 700; font-size: 11px; }
QLabel#StatCardTitle { font-size: 10px; font-weight: 700; color: #64748B; letter-spacing: 1px; }
QLabel#StatCardValue { font-size: 28px; font-weight: 800; margin-top: 2px; }
QLabel#DetailTitle { font-size: 18px; font-weight: 800; color: #0F172A; }
QLabel#DetailSubtitle { color: #64748B; font-size: 12px; }
QLabel#DetailSubHeader { color: #475569; font-weight: 700; font-size: 13px; margin-top: 16px; }

/* Frames */
QFrame#ChartFrame, QFrame#DetailPanel { background-color: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; }
QFrame#StatCard, QFrame#FilterFrame { background-color: #FFFFFF; border: 1px solid #E2E8F0; border-radius: 12px; }
QFrame#Separator { background-color: #E2E8F0; max-height: 1px; }

/* Sidebar */
#Sidebar { background-color: #FFFFFF; border-right: 1px solid #E2E8F0; min-width: 220px; max-width: 220px; }
#SidebarTitle { font-size: 18px; font-weight: 800; color: #0F172A; padding: 24px 20px 8px 20px; }
#SidebarSubtitle { font-size: 10px; font-weight: 600; color: #94A3B8; padding: 0px 20px 16px 20px; letter-spacing: 1px; }

QPushButton#NavItem_dashboard, QPushButton#NavItem_table, QPushButton#NavItem_sync,
QPushButton#NavItem_book, QPushButton#NavItem_settings, QPushButton#NavItem_theme {
    text-align: left;
    padding: 10px 16px;
    border-radius: 8px;
    margin: 2px 12px;
    border: none;
    font-weight: 600;
    font-size: 13px;
    color: #64748B;
    qproperty-iconSize: 20px 20px;
}
#NavItem_dashboard:hover, #NavItem_table:hover, #NavItem_sync:hover,
#NavItem_book:hover, #NavItem_settings:hover, #NavItem_theme:hover {
    background-color: #F1F5F9;
    color: #0F172A;
}
#NavItem_dashboard[active="true"], #NavItem_table[active="true"], #NavItem_sync[active="true"],
#NavItem_book[active="true"], #NavItem_settings[active="true"] {
    background-color: rgba(99, 102, 241, 0.1);
    color: #4F46E5;
    font-weight: 700;
}

/* Buttons */
QPushButton#PrimaryBtn { background-color: #4F46E5; color: white; padding: 10px 20px; border-radius: 8px; font-weight: 700; font-size: 13px; border: none; }
QPushButton#PrimaryBtn:hover { background-color: #4338CA; }
QPushButton#PrimaryBtn:disabled { background-color: #E2E8F0; color: #94A3B8; }

QPushButton#SuccessBtn { background-color: #059669; color: white; padding: 10px 20px; border-radius: 8px; font-weight: 700; font-size: 13px; border: none; }
QPushButton#SuccessBtn:hover { background-color: #047857; }
QPushButton#SuccessBtn:disabled { background-color: #E2E8F0; color: #94A3B8; }

QPushButton#DangerBtn { background-color: #DC2626; color: white; padding: 10px 20px; border-radius: 8px; font-weight: 700; font-size: 13px; border: none; }
QPushButton#DangerBtn:hover { background-color: #B91C1C; }
QPushButton#DangerBtn:disabled { background-color: #E2E8F0; color: #94A3B8; }

QPushButton#InfoBtn { background-color: #2563EB; color: white; padding: 10px 20px; border-radius: 8px; font-weight: 700; font-size: 13px; border: none; }
QPushButton#InfoBtn:hover { background-color: #1D4ED8; }
QPushButton#InfoBtn:disabled { background-color: #E2E8F0; color: #94A3B8; }

QPushButton#ActionBtn { background-color: #7C3AED; color: white; padding: 10px 20px; border-radius: 8px; font-weight: 700; font-size: 13px; border: none; }
QPushButton#ActionBtn:hover { background-color: #6D28D9; }
QPushButton#ActionBtn:disabled { background-color: #E2E8F0; color: #94A3B8; }

QPushButton#OutlineDangerBtn { background-color: transparent; color: #EF4444; border: 1px solid #EF4444; padding: 10px 20px; border-radius: 8px; font-weight: 700; font-size: 13px; }
QPushButton#OutlineDangerBtn:hover { background-color: rgba(239, 68, 68, 0.05); }

/* Tables */
QTableWidget, QTableView { background-color: #FFFFFF; alternate-background-color: #F8FAFC; border: 1px solid #E2E8F0; border-radius: 8px; gridline-color: #E2E8F0; color: #1E293B; selection-background-color: rgba(79, 70, 229, 0.15); }
QHeaderView::section { background-color: #F1F5F9; color: #64748B; padding: 8px 6px; border: none; border-bottom: 1px solid #E2E8F0; font-weight: 700; font-size: 11px; }

/* Scroll */
QScrollBar:vertical { border: none; background: transparent; width: 6px; margin: 4px 2px; }
QScrollBar::handle:vertical { background: #CBD5E1; border-radius: 3px; min-height: 30px; }
QScrollBar::handle:vertical:hover { background: #94A3B8; }

/* Combo */
QComboBox { background: #FFFFFF; border: 1px solid #E2E8F0; padding: 6px 10px; border-radius: 6px; color: #1E293B; font-weight: 600; font-size: 11px; }
QComboBox::drop-down { border: none; width: 20px; }
QComboBox QAbstractItemView { background: #FFFFFF; color: #1E293B; selection-background-color: #EEF2FF; border: 1px solid #E2E8F0; }

/* Progress */
QProgressBar { height: 20px; border-radius: 10px; background: #E2E8F0; text-align: center; color: #1E293B; font-weight: 700; border: none; }
QProgressBar::chunk { background: qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 #4F46E5, stop:1 #818CF8); border-radius: 10px; }

/* Inputs */
QLineEdit { background: #FFFFFF; border: 1px solid #E2E8F0; padding: 8px 12px; border-radius: 6px; color: #1E293B; font-size: 13px; }
QLineEdit:focus { border-color: #6366F1; }
QTextEdit { background-color: #FFFFFF; border: 1px solid #E2E8F0; color: #1E293B; font-family: 'Cascadia Code', 'Consolas', monospace; font-size: 12px; padding: 8px; border-radius: 8px; }

#VersionLabel { font-size: 10px; color: #94A3B8; padding: 8px 20px; }
#VersionLabel { font-size: 10px; color: #94A3B8; padding: 8px 20px; }
"""

class DataLoadWorker(QThread):
    """Background thread for initial data loading."""
    progress = Signal(str)
    percent = Signal(int)
    finished = Signal(bool)

    def __init__(self, engine):
        super().__init__()
        self.engine = engine

    def run(self):
        self.engine.initialize_db()
        
        # Load config to check data source mode
        from core.config import Config
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config = Config(os.path.join(base_dir, 'config.json'))
        
        source_mode = config.get("data_source", "local")
        
        if source_mode == "remote":
            ranking_url = config.get("remote_ranking_url")
            dd_url = config.get("remote_drilldown_url")
            
            if ranking_url:
                self.progress.emit("リモートランキングデータを取得中...")
                self.engine.load_from_url(ranking_url, is_drilldown=False, progress_callback=lambda msg: self.progress.emit(msg))
            
            if dd_url:
                self.progress.emit("リモートドリルダウンデータを取得中...")
                self.engine.load_from_url(dd_url, is_drilldown=True, progress_callback=lambda msg: self.progress.emit(msg))
            
            self.percent.emit(100)
            self.finished.emit(True)
            return

        # Fallback to local logic
        if os.path.exists(self.engine.master_csv) or os.path.exists(self.engine.master_parquet):
            self.percent.emit(10)
            self.progress.emit("ランキングデータを読み込み中...")
            self.engine.reload_master_data(progress_callback=lambda msg: self.progress.emit(msg))
            self.percent.emit(60)
            
            if os.path.exists(self.engine.drilldown_csv) or os.path.exists(self.engine.drilldown_parquet):
                self.progress.emit("ドリルダウンデータを読み込み中...")
                self.engine.reload_drilldown_data(progress_callback=lambda msg: self.progress.emit(msg))
                
            self.percent.emit(100)
            self.finished.emit(True)
            
        elif os.path.exists(self.engine.zip_path):
            self.percent.emit(10)
            self.progress.emit("ZIPデータを展開中...")
            ok = self.engine.load_from_zip(self.engine.zip_path, progress_callback=lambda msg: self.progress.emit(msg))
            self.percent.emit(100)
            self.finished.emit(ok)
            
        else:
            self.progress.emit("データファイルが見つかりません。同期タブからデータを取得してください。")
            self.percent.emit(100)
            self.finished.emit(False)


class MainWindow(QMainWindow):
    def __init__(self, config, engine):
        super().__init__()
        self.config = config
        self.engine = engine
        self._is_dark = config.get("theme", "dark") == "dark"
        self._is_admin = False  # NEW: Admin mode state
        
        self.setWindowTitle("Streamd BI Native Engine")
        self.resize(1400, 850)
        
        # Central Widget
        central_widget = QWidget()
        central_widget.setObjectName("CentralWidget")
        self.setCentralWidget(central_widget)
        
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Sidebar
        self.sidebar = Sidebar()
        self.sidebar.set_theme_label(self._is_dark)
        main_layout.addWidget(self.sidebar)
        
        # Content Area
        self.content_stack = QStackedWidget()
        main_layout.addWidget(self.content_stack)
        
        # Loading Screen
        self.loading_widget = QWidget()
        self.loading_widget.setObjectName("LoadingWidget")
        
        loading_layout = QVBoxLayout(self.loading_widget)
        loading_layout.setAlignment(Qt.AlignCenter)
        loading_layout.setSpacing(12)
        
        loading_icon = QLabel("")
        loading_icon.setStyleSheet("font-size: 56px;")
        loading_icon.setAlignment(Qt.AlignCenter)
        loading_layout.addWidget(loading_icon)
        
        loading_title = QLabel("Streamd BI Engine")
        loading_title.setObjectName("PageHeader")
        loading_title.setAlignment(Qt.AlignCenter)
        loading_layout.addWidget(loading_title)
        
        loading_subtitle = QLabel("データを準備しています...")
        loading_subtitle.setObjectName("PageSubtitle")
        loading_subtitle.setAlignment(Qt.AlignCenter)
        loading_layout.addWidget(loading_subtitle)
        
        loading_layout.addSpacing(16)
        
        self.loading_pbar = QProgressBar()
        self.loading_pbar.setObjectName("LoadingBar")
        self.loading_pbar.setFixedWidth(360)
        self.loading_pbar.setFixedHeight(6)
        self.loading_pbar.setTextVisible(False)
        self.loading_pbar.setRange(0, 100)
        self.loading_pbar.setValue(0)
        
        pbar_container = QWidget()
        pbar_layout = QHBoxLayout(pbar_container)
        pbar_layout.setAlignment(Qt.AlignCenter)
        pbar_layout.addWidget(self.loading_pbar)
        loading_layout.addWidget(pbar_container)
        
        self.loading_status = QLabel("初期化中...")
        self.loading_status.setStyleSheet("font-size: 11px; color: #94A3B8; margin-top: 4px;")
        self.loading_status.setAlignment(Qt.AlignCenter)
        loading_layout.addWidget(self.loading_status)
        
        self.content_stack.addWidget(self.loading_widget)
        
        # Connect sidebar navigation
        self.sidebar.nav_changed.connect(self.switch_view)
        self.sidebar.theme_toggled.connect(self.toggle_theme)
        
        # Views will be set up after data loads
        self.views = {}
        self._views_initialized = False
        
        # File System Watcher
        self._file_watcher = QFileSystemWatcher()
        self._file_watcher.fileChanged.connect(self._on_file_changed)
        
        self._reload_debounce = QTimer()
        self._reload_debounce.setSingleShot(True)
        self._reload_debounce.setInterval(2000)
        self._reload_debounce.timeout.connect(self._do_auto_reload)
        
        # Sleep Prevention (Windows only)
        self._prevent_sleep(True)

    def _prevent_sleep(self, prevent=True):
        """Prevents the system from entering sleep mode while the app is active (Windows only)."""
        if os.name == 'nt':
            try:
                # ES_CONTINUOUS | ES_SYSTEM_REQUIRED
                ES_CONTINUOUS = 0x80000000
                ES_SYSTEM_REQUIRED = 0x00000001
                
                flags = ES_CONTINUOUS
                if prevent:
                    flags |= ES_SYSTEM_REQUIRED
                
                ctypes.windll.kernel32.SetThreadExecutionState(flags)
                print(f"[MAIN] Sleep prevention {'enabled' if prevent else 'disabled'}.")
            except Exception as e:
                print(f"[MAIN] Failed to set sleep state: {e}")

    def closeEvent(self, event):
        # Restore sleep state on exit
        self._prevent_sleep(False)
        super().closeEvent(event)

    def keyPressEvent(self, event):
        """Handle global hotkeys."""
        # Ctrl + Shift + L for Admin Mode
        if (event.modifiers() & Qt.ControlModifier and 
            event.modifiers() & Qt.ShiftModifier and 
            event.key() == Qt.Key_L):
            
            pwd, ok = QInputDialog.getText(self, "管理者認証", "パスワードを入力してください:", QLineEdit.Password)
            if ok and pwd == "Ring0#1102":
                self._is_admin = True
                QMessageBox.information(self, "認証成功", "管理者モードが有効になりました。設定画面のロックが解除されます。")
                # Update settings view if initialized
                if "settings" in self.views:
                    self.views["settings"].set_admin_mode(True)
            elif ok:
                QMessageBox.warning(self, "認証失敗", "パスワードが正しくありません。")
            return

        super().keyPressEvent(event)

    def _setup_file_watcher(self):
        for path in [self.engine.master_csv, self.engine.drilldown_csv]:
            if os.path.exists(path):
                self._file_watcher.addPath(path)

    def _on_file_changed(self, path):
        if os.path.exists(path) and path not in self._file_watcher.files():
            self._file_watcher.addPath(path)
        self._reload_debounce.start()

    def _do_auto_reload(self):
        try:
            if os.path.exists(self.engine.master_csv) or os.path.exists(self.engine.master_parquet):
                self.engine.reload_master_data()
            if os.path.exists(self.engine.drilldown_csv) or os.path.exists(self.engine.drilldown_parquet):
                self.engine.reload_drilldown_data()
            self._on_data_reloaded()
            print("[WATCHER] Auto-reload complete.")
        except Exception as e:
            print(f"[WATCHER] Auto-reload error: {e}")

    def load_data_async(self):
        self.loader = DataLoadWorker(self.engine)
        self.loader.progress.connect(self._on_load_progress)
        self.loader.percent.connect(self._on_load_percent)
        self.loader.finished.connect(self._on_load_finished)
        self.loader.start()

    def _on_load_progress(self, msg):
        self.loading_status.setText(msg)

    def _on_load_percent(self, pct):
        self.loading_pbar.setValue(pct)

    def _on_load_finished(self, success):
        self._setup_views()
        self._setup_file_watcher()
        self.sidebar.set_active_item("dashboard")

    def _setup_views(self):
        if self._views_initialized:
            return
            
        from ui.views.dashboard import DashboardView
        from ui.views.ranking import RankingView
        from ui.views.sync import SyncView
        from ui.views.codemap import CodeMapView
        from ui.views.settings import SettingsView
        
        self.views["dashboard"] = DashboardView(self.engine)
        self.views["ranking"] = RankingView(self.config, self.engine)
        self.views["sync"] = SyncView(self.config, self.engine)
        self.views["codemap"] = CodeMapView(self.config)
        self.views["settings"] = SettingsView(self.config)
        self.views["settings"].set_admin_mode(self._is_admin)
        self.views["settings"].settings_saved.connect(self._on_settings_saved)
        
        self.views["sync"].data_loaded.connect(self._on_data_reloaded)
        
        if hasattr(self.views["ranking"], "client_selected"):
            self.views["ranking"].client_selected.connect(self._on_client_selected)
            
        if hasattr(self.views["dashboard"], "point_clicked"):
            self.views["dashboard"].point_clicked.connect(self._on_dashboard_point_clicked)
            
        for view in self.views.values():
            if hasattr(view, "set_theme"):
                view.set_theme(self._is_dark)
            self.content_stack.addWidget(view)
            
        self._views_initialized = True

    def _on_settings_saved(self):
        """Handle settings save by triggering a full data reload."""
        print("[MAIN] Settings saved, triggering data reload...")
        # Return to loading screen
        self.content_stack.setCurrentWidget(self.loading_widget)
        self.loading_pbar.setValue(0)
        self.loading_status.setText("設定を反映中...")
        # Re-run loader
        self.load_data_async()

    def _on_data_reloaded(self):
        self._setup_file_watcher()
        for view in self.views.values():
            if hasattr(view, "refresh"):
                view.refresh()

    def _on_client_selected(self, client_id, enterprise_name):
        if "dashboard" in self.views:
            dashboard = self.views["dashboard"]
            if hasattr(dashboard, "show_client_trend"):
                dashboard.show_client_trend(client_id, enterprise_name)
            self.sidebar.set_active_item("dashboard")

    def _on_dashboard_point_clicked(self, filters):
        if "ranking" not in self.views:
            return
            
        self._cleanup_tooltips()
        self.sidebar.set_active_item("ranking")
        ranking_view = self.views["ranking"]
        where_parts = []
        
        if filters.get("client_id"):
            where_parts.append(f"\"クライアントID\" = '{filters['client_id']}'")
        if filters.get("month"):
            where_parts.append(f"\"処理月\" = '{filters['month']}'")
        if filters.get("vtype"):
            where_parts.append(f"\"証憑タイプ\" = '{filters['vtype']}'")
            
        if where_parts:
            clause = "WHERE " + " AND ".join(where_parts)
            ranking_view.search_input.setText(clause)
            
        if ranking_view.is_summary_mode:
            ranking_view.set_mode(False)
        else:
            ranking_view.refresh()

    def _cleanup_tooltips(self):
        for view in self.views.values():
            if hasattr(view, "custom_tooltip"):
                view.custom_tooltip.hide()
        from PySide6.QtWidgets import QToolTip
        QToolTip.hideText()

    def switch_view(self, view_id):
        if not self._views_initialized:
            return
            
        if view_id in self.views:
            self._cleanup_tooltips()
            self.content_stack.setCurrentWidget(self.views[view_id])
            if hasattr(self.views[view_id], "refresh"):
                self.views[view_id].refresh()

    def toggle_theme(self):
        self._is_dark = not self._is_dark
        from PySide6.QtWidgets import QApplication
        qapp = QApplication.instance()
        
        if self._is_dark:
            style_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ui', 'styles.qss')
            if os.path.exists(style_path):
                with open(style_path, 'r', encoding='utf-8') as f:
                    qss = f.read()
                icons_abs = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'ui', 'assets', 'icons').replace('\\', '/')
                qss = qss.replace('url("assets/icons/', f'url("{icons_abs}/')
                qapp.setStyleSheet(qss)
            self.sidebar.set_theme_label(True)
        else:
            qapp.setStyleSheet(LIGHT_THEME)
            self.sidebar.set_theme_label(False)
            
        # Force palette update for tooltips
        from PySide6.QtGui import QPalette, QColor
        palette = qapp.palette()
        if self._is_dark:
            base = QColor("#1E293B")
            text = QColor("#F8FAFC")
        else:
            base = QColor("#FFFFFF")
            text = QColor("#1E293B")
            
        palette.setColor(QPalette.ToolTipBase, base)
        palette.setColor(QPalette.ToolTipText, text)
        palette.setColor(QPalette.Window, base) # Tooltip is a window
        palette.setColor(QPalette.WindowText, text)
        qapp.setPalette(palette)

        # Hide any active tooltip so it doesn't linger with the previous theme color
        QToolTip.hideText()
        
        for view in self.views.values():
            if hasattr(view, "set_theme"):
                view.set_theme(self._is_dark)
                
        self.config.save({"theme": "dark" if self._is_dark else "light"})