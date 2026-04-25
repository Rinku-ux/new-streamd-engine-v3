"""
Streamd BI - Heatmap View
Client x Month accuracy matrix with color gradient visualization.
Paginated rendering to prevent UI freeze with large datasets.
"""
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                               QTableWidget, QTableWidgetItem, QFrame,
                               QComboBox, QHeaderView, QLineEdit, QPushButton,
                               QSpinBox)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QColor, QBrush, QFont
import math


METRIC_OPTIONS = [
    ("overall", "総合精度"),
    ("date", "日付精度"),
    ("amount", "金額精度"),
    ("supplier", "支払先精度"),
    ("regnum", "登録番号精度"),
    ("tax", "税区分精度"),
    ("account", "科目精度"),
    ("content", "内容精度"),
]

PAGE_SIZE = 50  # Rows per page


def accuracy_color(value, is_dark=True):
    """Return QColor for an accuracy value (0-100) using red→yellow→green gradient."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return QColor("#1E293B" if is_dark else "#F1F5F9")

    v = max(0, min(100, float(value)))
    if v >= 95:
        return QColor("#059669")
    elif v >= 85:
        r = int(5 + (95 - v) * 25)
        return QColor(r, 150, 100)
    elif v >= 70:
        return QColor("#F59E0B")
    elif v >= 50:
        return QColor("#F97316")
    else:
        return QColor("#EF4444")


class HeatmapView(QWidget):
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.setObjectName("HeatmapView")
        self.engine = engine
        self._is_dark = True
        self._data = None
        self._filtered_data = None
        self._search_text = ""
        self._current_page = 0
        self._total_pages = 0
        self._data_loaded = False

        layout = QVBoxLayout(self)
        layout.setContentsMargins(32, 24, 32, 32)
        layout.setSpacing(16)

        # Header
        header_row = QHBoxLayout()
        header_row.setSpacing(12)

        from ui.components.icons import Icons
        icon_label = QLabel()
        icon_label.setPixmap(Icons.get_pixmap(Icons.HEATMAP, 24, "#818CF8"))
        header_row.addWidget(icon_label)

        header = QLabel("Accuracy Heatmap")
        header.setObjectName("PageHeader")
        header_row.addWidget(header)
        header_row.addStretch()
        layout.addLayout(header_row)

        subtitle = QLabel("クライアント × 月  精度マトリクス")
        subtitle.setObjectName("PageSubtitle")
        layout.addWidget(subtitle)

        # Controls
        ctrl_frame = QFrame()
        ctrl_frame.setObjectName("FilterFrame")
        ctrl_layout = QHBoxLayout(ctrl_frame)
        ctrl_layout.setContentsMargins(16, 10, 16, 10)
        ctrl_layout.setSpacing(12)

        # Search
        search_icon = QLabel()
        search_icon.setPixmap(Icons.get_pixmap(Icons.SEARCH, 16, "#94A3B8"))
        ctrl_layout.addWidget(search_icon)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("クライアント名 / ID で絞り込み...")
        self.search_input.setMinimumWidth(220)
        self.search_input.setStyleSheet("border: none; background: transparent; font-size: 13px;")
        self.search_input.textChanged.connect(self._on_search_changed)
        ctrl_layout.addWidget(self.search_input)

        clear_btn = QPushButton("✕")
        clear_btn.setFixedSize(28, 28)
        clear_btn.setStyleSheet(
            "QPushButton { border: none; background: transparent; color: #64748B; font-size: 14px; font-weight: 700; }"
            "QPushButton:hover { color: #EF4444; }"
        )
        clear_btn.clicked.connect(lambda: self.search_input.clear())
        ctrl_layout.addWidget(clear_btn)

        ctrl_layout.addSpacing(16)

        # Metric selector
        metric_label = QLabel("メトリクス:")
        metric_label.setObjectName("FilterLabel")
        ctrl_layout.addWidget(metric_label)

        self.metric_combo = QComboBox()
        for code, label in METRIC_OPTIONS:
            self.metric_combo.addItem(label, code)
        self.metric_combo.currentIndexChanged.connect(self._on_metric_changed)
        ctrl_layout.addWidget(self.metric_combo)

        ctrl_layout.addStretch()

        self.info_label = QLabel("")
        self.info_label.setStyleSheet("color: #94A3B8; font-size: 11px;")
        ctrl_layout.addWidget(self.info_label)
        layout.addWidget(ctrl_frame)

        # Heatmap Table
        self.table = QTableWidget()
        self.table.setAlternatingRowColors(False)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.verticalHeader().setDefaultSectionSize(28)
        self.table.horizontalHeader().setDefaultSectionSize(70)
        self.table.setStyleSheet("QTableWidget { gridline-color: #334155; }")
        layout.addWidget(self.table)

        # Pagination controls
        page_frame = QFrame()
        page_frame.setObjectName("FilterFrame")
        page_layout = QHBoxLayout(page_frame)
        page_layout.setContentsMargins(16, 8, 16, 8)
        page_layout.setSpacing(10)

        self.prev_btn = QPushButton("◀ 前")
        self.prev_btn.setStyleSheet(
            "QPushButton { border: 1px solid #475569; background: transparent; color: #94A3B8; "
            "padding: 4px 12px; border-radius: 4px; font-weight: 700; font-size: 11px; }"
            "QPushButton:hover { background: rgba(99,102,241,0.1); color: #818CF8; }"
            "QPushButton:disabled { color: #334155; border-color: #334155; }"
        )
        self.prev_btn.clicked.connect(self._prev_page)
        page_layout.addWidget(self.prev_btn)

        self.page_label = QLabel("1 / 1")
        self.page_label.setStyleSheet("color: #CBD5E1; font-weight: 700; font-size: 12px;")
        page_layout.addWidget(self.page_label)

        self.next_btn = QPushButton("次 ▶")
        self.next_btn.setStyleSheet(self.prev_btn.styleSheet())
        self.next_btn.clicked.connect(self._next_page)
        page_layout.addWidget(self.next_btn)

        page_layout.addStretch()

        self.total_label = QLabel("")
        self.total_label.setStyleSheet("color: #64748B; font-size: 11px;")
        page_layout.addWidget(self.total_label)
        layout.addWidget(page_frame)

    def set_theme(self, is_dark):
        self._is_dark = is_dark
        bg = "#F8FAFC" if not is_dark else "#0F172A"
        self.setStyleSheet(f"QWidget#HeatmapView {{ background-color: {bg}; }}")
        if self._filtered_data is not None:
            self._update_theme_colors()

    def _update_theme_colors(self):
        """Update only the background colors of existing cells to prevent UI freeze during theme toggle."""
        df = self._filtered_data
        if df is None or df.empty or self.table.rowCount() == 0:
            return
            
        start = self._current_page * PAGE_SIZE
        end = min(start + PAGE_SIZE, len(df))
        page_df = df.iloc[start:end]

        self.table.setUpdatesEnabled(False)
        self.table.blockSignals(True)
        
        default_bg = QColor("#1E293B" if self._is_dark else "#F1F5F9")
        
        for row_idx in range(self.table.rowCount()):
            for col_idx in range(self.table.columnCount()):
                item = self.table.item(row_idx, col_idx)
                if not item:
                    continue
                    
                # If item has a value (e.g. 50.0%), get the color using the data
                try:
                    val = page_df.iloc[row_idx, col_idx]
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        item.setBackground(QBrush(accuracy_color(val, self._is_dark)))
                    else:
                        item.setBackground(QBrush(default_bg))
                except (IndexError, KeyError):
                    item.setBackground(QBrush(default_bg))

        self.table.blockSignals(False)
        self.table.setUpdatesEnabled(True)

    def refresh(self):
        if not self.engine.has_data():
            return
        if not self._data_loaded:
            QTimer.singleShot(100, self._load_data)

    def _on_metric_changed(self, idx):
        self._data_loaded = False
        QTimer.singleShot(100, self._load_data)

    def _on_search_changed(self, text):
        self._search_text = text.strip().lower()
        if not hasattr(self, '_search_timer'):
            self._search_timer = QTimer()
            self._search_timer.setSingleShot(True)
            self._search_timer.timeout.connect(self._apply_filter)
        self._search_timer.start(400)

    def _load_data(self):
        metric = self.metric_combo.currentData() or "overall"
        self._data = self.engine.get_heatmap_data(metric=metric, limit=0)
        self._data_loaded = True
        self._apply_filter()

    def _apply_filter(self):
        df = self._data
        if df is None or df.empty:
            self._filtered_data = df
            self._current_page = 0
            self._total_pages = 0
            self._render_page()
            return

        if self._search_text:
            mask = []
            for idx_val in df.index:
                if isinstance(idx_val, tuple):
                    cid = str(idx_val[0]).lower()
                    name = str(idx_val[1]).lower() if idx_val[1] else ""
                    match = self._search_text in cid or self._search_text in name
                else:
                    match = self._search_text in str(idx_val).lower()
                mask.append(match)
            self._filtered_data = df[mask]
        else:
            self._filtered_data = df

        self._current_page = 0
        self._total_pages = max(1, math.ceil(len(self._filtered_data) / PAGE_SIZE))
        self._render_page()

    def _prev_page(self):
        if self._current_page > 0:
            self._current_page -= 1
            self._render_page()

    def _next_page(self):
        if self._current_page < self._total_pages - 1:
            self._current_page += 1
            self._render_page()

    def _render_page(self):
        df = self._filtered_data
        if df is None or df.empty:
            self.table.clear()
            self.table.setRowCount(0)
            self.table.setColumnCount(0)
            total = len(self._data) if self._data is not None else 0
            self.info_label.setText(f"0 / {total} クライアント" if self._search_text else "データなし")
            self.page_label.setText("0 / 0")
            self.prev_btn.setEnabled(False)
            self.next_btn.setEnabled(False)
            self.total_label.setText("")
            return

        # Slice for current page
        start = self._current_page * PAGE_SIZE
        end = min(start + PAGE_SIZE, len(df))
        page_df = df.iloc[start:end]

        months = list(page_df.columns)
        clients = page_df.index.tolist()

        # Block signals during bulk update
        self.table.setUpdatesEnabled(False)
        self.table.blockSignals(True)

        self.table.clear()
        self.table.setRowCount(len(clients))
        self.table.setColumnCount(len(months))
        self.table.setHorizontalHeaderLabels(months)

        v_labels = []
        for c in clients:
            if isinstance(c, tuple):
                v_labels.append(f"{c[1]} ({c[0]})" if c[1] else str(c[0]))
            else:
                v_labels.append(str(c))
        self.table.setVerticalHeaderLabels(v_labels)

        for row_idx in range(len(clients)):
            for col_idx in range(len(months)):
                try:
                    val = page_df.iloc[row_idx, col_idx]
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        item = QTableWidgetItem(f"{val:.1f}%")
                        item.setBackground(QBrush(accuracy_color(val, self._is_dark)))
                        item.setForeground(QBrush(QColor("#FFFFFF")))
                    else:
                        item = QTableWidgetItem("—")
                        item.setBackground(QBrush(QColor("#1E293B" if self._is_dark else "#F1F5F9")))
                        item.setForeground(QBrush(QColor("#475569")))
                except (IndexError, KeyError):
                    item = QTableWidgetItem("—")

                item.setTextAlignment(Qt.AlignCenter)
                item.setFont(QFont("Segoe UI", 9, QFont.Bold))
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                self.table.setItem(row_idx, col_idx, item)

        self.table.blockSignals(False)
        self.table.setUpdatesEnabled(True)

        # Update pagination controls
        total_all = len(self._data) if self._data is not None else len(df)
        total_filtered = len(df)
        self.page_label.setText(f"{self._current_page + 1} / {self._total_pages}")
        self.prev_btn.setEnabled(self._current_page > 0)
        self.next_btn.setEnabled(self._current_page < self._total_pages - 1)

        if self._search_text:
            self.info_label.setText(f"{total_filtered} / {total_all} 件 (検索中)")
        else:
            self.info_label.setText(f"{total_filtered} クライアント × {len(months)} ヶ月")
        self.total_label.setText(f"表示: {start+1}〜{end} / {total_filtered}")
