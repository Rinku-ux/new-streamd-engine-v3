from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                               QFrame, QGridLayout, QScrollArea, QCheckBox,
                               QComboBox, QPushButton, QFileDialog, QMessageBox)
from PySide6.QtCore import Qt, QTimer, QPointF, QMargins, Signal
from PySide6.QtGui import QColor, QPen, QFont, QPainter
from PySide6.QtCharts import (QChart, QChartView, QLineSeries, QValueAxis,
                              QCategoryAxis)
import math
from collections import defaultdict

# Voucher type mapping. "nondigitization" is removed per spec.
# "medical" is kept but shown only when actual data exists (data-driven).
VOUCHER_MAP_ORDERED = [
    ("receipt",       "領収書",           "#818CF8"),
    ("invoice",       "請求書",           "#F472B6"),
    ("medical",       "医療費",           "#38BDF8"),
    ("bankbook",      "通帳",             "#34D399"),
    ("creditcard",    "クレジットカード", "#FBBF24"),
    ("cashbook",      "現金出納帳",       "#FB923C"),
    ("totaltransfer", "総合振込",         "#A78BFA"),
    ("transferslip",  "振替伝票",         "#22D3EE"),
    ("depositslip",   "入金伝票",         "#F87171"),
    ("paymentslip",   "出金伝票",         "#4ADE80"),
]

VOUCHER_MAP = {code: (label, color) for code, label, color in VOUCHER_MAP_ORDERED}

METRIC_DEFS_ORDERED = [
    ("overall",  "総合精度",     "対象仕訳数",   "全体正解件数"),
    ("date",     "日付精度",     "日付_対象",    "日付_正解"),
    ("amount",   "金額精度",     "金額_対象",    "金額_正解"),
    ("supplier", "支払先精度",   "支払先_対象",  "支払先_正解"),
    ("regnum",   "登録番号精度", "登録_対象",    "登録_正解"),
    ("tax",      "税区分精度",   "税区分_対象",  "税区分_正解"),
    ("account",  "科目精度",     "科目_対象",    "科目_正解"),
    ("content",  "内容精度",     "内容_対象",    "内容_正解"),
]

METRIC_COLORS = [
    "#818CF8", "#F472B6", "#FBBF24", "#34D399",
    "#FB923C", "#A78BFA", "#22D3EE", "#F87171",
]

RANGE_PRESETS = [
    ("全期間", None),
    ("直近 3ヶ月", 3),
    ("直近 6ヶ月", 6),
    ("直近 12ヶ月", 12),
    ("指定範囲", -1),
]


class ChartTooltip(QFrame):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(Qt.ToolTip | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_ShowWithoutActivating)
        self.setObjectName("ChartTooltip")
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(10, 8, 10, 8)
        self.label = QLabel("")
        self.label.setStyleSheet("font-size: 11px; font-weight: 600; line-height: 1.4;")
        layout.addWidget(self.label)
        
        self.set_theme(True) # Default dark
        
        self.hide_timer = QTimer(self)
        self.hide_timer.setSingleShot(True)
        self.hide_timer.timeout.connect(self.hide)
        self.hide()

    def set_theme(self, is_dark):
        from PySide6.QtGui import QPalette, QColor
        if is_dark:
            bg_color = QColor("#1E293B")
            text_color = QColor("#F8FAFC")
            border = "#475569"
        else:
            bg_color = QColor("#FFFFFF")
            text_color = QColor("#1E293B")
            border = "#CBD5E1"
            
        # Set palette as a fallback
        pal = self.palette()
        pal.setColor(QPalette.Window, bg_color)
        pal.setColor(QPalette.WindowText, text_color)
        pal.setColor(QPalette.Base, bg_color)
        pal.setColor(QPalette.Text, text_color)
        self.setPalette(pal)
            
        self.setStyleSheet(f"""
            QFrame#ChartTooltip {{
                background-color: {bg_color.name()} !important;
                border: 1px solid {border} !important;
                border-radius: 6px;
            }}
            QLabel {{
                color: {text_color.name()} !important;
                background: transparent !important;
                font-family: 'Segoe UI', 'Inter';
            }}
        """)

    def show_text(self, pos, text):
        # Safety: Do not show if the owner view is hidden
        parent = self.parent()
        if parent and not parent.isVisible():
            self.hide()
            return

        self.label.setText(text)
        self.adjustSize()
        # Offset to not be directly under the cursor
        self.move(pos.x() + 15, pos.y() + 15)
        self.show()
        self.raise_()
        self.hide_timer.start(3000) # Auto-hide after 3 seconds if not moved

def _safe_int(val):
    if val is None:
        return 0
    try:
        if isinstance(val, float) and math.isnan(val):
            return 0
        return int(val)
    except (ValueError, TypeError):
        return 0


class StatCard(QFrame):
    def __init__(self, title, value, color="#818CF8", parent=None):
        super().__init__(parent)
        self._color = color
        self.setObjectName("StatCard")
        self.setMinimumHeight(110)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(2)
        
        lbl = QLabel(title)
        lbl.setObjectName("StatCardTitle")
        layout.addWidget(lbl)
        
        val_row = QHBoxLayout()
        val_row.setSpacing(8)
        self.value_label = QLabel(str(value))
        self.value_label.setObjectName("StatCardValue")
        self.value_label.setStyleSheet(f"color: {color};")
        val_row.addWidget(self.value_label)
        val_row.addStretch()
        layout.addLayout(val_row)
        
        self.trend_label = QLabel("")
        self.trend_label.setObjectName("StatCardTrend")
        layout.addWidget(self.trend_label)

    def set_value(self, value, trend=None):
        self.value_label.setText(str(value))
        if trend is not None:
            try:
                trend_val = float(trend.replace('%', '').replace('+', ''))
                if trend_val > 0:
                    self.trend_label.setText(f"▲ +{abs(trend_val):.1f}%")
                    self.trend_label.setStyleSheet("color: #10B981;")
                elif trend_val < 0:
                    self.trend_label.setText(f"▼ -{abs(trend_val):.1f}%")
                    self.trend_label.setStyleSheet("color: #EF4444;")
                else:
                    self.trend_label.setText("— 0.0%")
                    self.trend_label.setStyleSheet("color: #64748B;")
            except Exception:
                self.trend_label.setText("")
        else:
            self.trend_label.setText("")


def create_theme_chart(title="", is_dark=True):
    chart = QChart()
    chart.setTitle(title)
    bg_color = QColor("#0F172A") if is_dark else QColor("#FFFFFF")
    text_color = QColor("#F8FAFC") if is_dark else QColor("#1E293B")
    legend_color = QColor("#CBD5E1") if is_dark else QColor("#475569")
    chart.setBackgroundBrush(bg_color)
    chart.setTitleBrush(text_color)
    chart.setTitleFont(QFont("Segoe UI", 14, QFont.Bold))
    chart.legend().setLabelColor(legend_color)
    chart.legend().setFont(QFont("Segoe UI", 9))
    chart.legend().setAlignment(Qt.AlignBottom)
    chart.setMargins(QMargins(12, 8, 12, 8))
    chart.setAnimationOptions(QChart.NoAnimation)
    return chart


def update_chart_theme(chart, is_dark=True):
    bg_color = QColor("#0F172A") if is_dark else QColor("#FFFFFF")
    text_color = QColor("#F8FAFC") if is_dark else QColor("#1E293B")
    legend_color = QColor("#CBD5E1") if is_dark else QColor("#475569")
    chart.setBackgroundBrush(bg_color)
    chart.setTitleBrush(text_color)
    chart.legend().setLabelColor(legend_color)
    
    grid_color = QColor("#1E293B") if is_dark else QColor("#E2E8F0")
    line_color = QColor("#334155") if is_dark else QColor("#CBD5E1")
    label_color = QColor("#94A3B8") if is_dark else QColor("#475569")
    
    for axis in chart.axes():
        axis.setLabelsColor(label_color)
        axis.setGridLineColor(grid_color)
        axis.setLinePenColor(line_color)


class DashboardView(QWidget):
    point_clicked = Signal(dict)

    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.setObjectName("DashboardView")
        self.engine = engine
        self.series_map = {}
        self.checkboxes = {}
        self.metric_series_map = {}
        self.metric_checkboxes = {}
        self._all_months = []
        self._overall_months = []
        self._voucher_months = []
        self._vtype_metric_months = []
        self._client_months = []
        self._cache_hash = None
        self._client_filter = None
        self._is_dark = True
        self._present_vtypes = set()
        
        # Custom tooltip instance (parented to this view)
        self.custom_tooltip = ChartTooltip(self)
        
        self.scroll = QScrollArea()
        self.scroll.setObjectName("DashboardScroll")
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QFrame.NoFrame)
        
        container = QWidget()
        container.setObjectName("DashboardContainer")
        main_layout = QVBoxLayout(container)
        main_layout.setContentsMargins(32, 24, 32, 32)
        main_layout.setSpacing(20)
        
        # Header
        header_row = QHBoxLayout()
        header_row.setSpacing(12)
        
        from ui.components.icons import Icons
        icon_label = QLabel()
        icon_label.setPixmap(Icons.get_pixmap(Icons.DASHBOARD, 24, "#818CF8" if self._is_dark else "#4F46E5"))
        header_row.addWidget(icon_label)
        
        header_lbl = QLabel("Dashboard")
        header_lbl.setObjectName("DashboardHeader")
        header_row.addWidget(header_lbl)
        
        header_row.addStretch() # Fix: Push text to the left together with icon
        
        self.last_sync_label = QLabel("Last Sync: --")
        self.last_sync_label.setStyleSheet("color: #94A3B8; font-size: 11px; margin-right: 15px; font-family: 'Segoe UI';")
        header_row.addWidget(self.last_sync_label, 0, Qt.AlignBottom)
        
        self.sync_progress_label = QLabel("Sync Progress: --")
        self.sync_progress_label.setStyleSheet("color: #94A3B8; font-size: 11px; margin-right: 10px; font-family: 'Segoe UI';")
        header_row.addWidget(self.sync_progress_label, 0, Qt.AlignBottom)
        
        main_layout.addLayout(header_row)
        
        subtitle = QLabel("Streamd BI - 全社推移グラフ")
        subtitle.setObjectName("PageSubtitle")
        main_layout.addWidget(subtitle)
        
        # Stats Cards
        stats_grid = QGridLayout()
        stats_grid.setSpacing(16)
        self.card_clients = StatCard("ACTIVE CLIENTS", "---", "#818CF8")
        self.card_accuracy = StatCard("OVERALL ACCURACY", "---", "#10B981")
        self.card_vouchers = StatCard("TOTAL VOUCHERS", "---", "#F59E0B")
        self.card_rows = StatCard("DATA ROWS", "---", "#EC4899")
        
        stats_grid.addWidget(self.card_clients, 0, 0)
        stats_grid.addWidget(self.card_accuracy, 0, 1)
        stats_grid.addWidget(self.card_vouchers, 0, 2)
        stats_grid.addWidget(self.card_rows, 0, 3)
        main_layout.addLayout(stats_grid)
        
        # Month Range Filter (shared)
        range_frame = QFrame()
        range_frame.setObjectName("FilterFrame")
        range_layout = QHBoxLayout(range_frame)
        range_layout.setContentsMargins(16, 10, 16, 10)
        range_layout.setSpacing(10)
        
        range_label = QLabel(" 月範囲:")
        range_label.setObjectName("FilterLabel")
        range_layout.addWidget(range_label)
        
        self.range_preset_combo = QComboBox()
        for label, _ in RANGE_PRESETS:
            self.range_preset_combo.addItem(label)
        self.range_preset_combo.currentIndexChanged.connect(self._on_range_preset_changed)
        range_layout.addWidget(self.range_preset_combo)
        
        self.range_start_label = QLabel("開始:")
        self.range_start_label.setObjectName("FilterLabel")
        self.range_start_label.setVisible(False)
        range_layout.addWidget(self.range_start_label)
        
        self.range_start_combo = QComboBox()
        self.range_start_combo.setVisible(False)
        self.range_start_combo.currentIndexChanged.connect(self._on_range_changed)
        range_layout.addWidget(self.range_start_combo)
        
        self.range_end_label = QLabel("終了:")
        self.range_end_label.setObjectName("FilterLabel")
        self.range_end_label.setVisible(False)
        range_layout.addWidget(self.range_end_label)
        
        self.range_end_combo = QComboBox()
        self.range_end_combo.setVisible(False)
        self.range_end_combo.currentIndexChanged.connect(self._on_range_changed)
        range_layout.addWidget(self.range_end_combo)
        
        range_layout.addStretch()
        main_layout.addWidget(range_frame)
        
        # Chart 1: Overall
        chart1_frame = QFrame()
        chart1_frame.setObjectName("ChartFrame")
        chart1_layout = QVBoxLayout(chart1_frame)
        chart1_layout.setContentsMargins(12, 8, 12, 0)
        
        overall_ctrl = QHBoxLayout()
        overall_ctrl.setSpacing(8)
        overall_metric_label = QLabel(" 表示メトリクス:")
        overall_metric_label.setObjectName("FilterLabel")
        overall_ctrl.addWidget(overall_metric_label)
        
        self.overall_metric_combo = QComboBox()
        for _, label, _, _ in METRIC_DEFS_ORDERED:
            self.overall_metric_combo.addItem(label)
        self.overall_metric_combo.currentIndexChanged.connect(self._on_overall_metric_changed)
        overall_ctrl.addWidget(self.overall_metric_combo)
        overall_ctrl.addStretch()
        chart1_layout.addLayout(overall_ctrl)
        
        self.overall_chart = create_theme_chart(" 全社推移グラフ (1本線)", self._is_dark)
        self.overall_chart_view = QChartView(self.overall_chart)
        self.overall_chart_view.setRenderHint(QPainter.Antialiasing)
        self.overall_chart_view.setMinimumHeight(320)
        self.overall_chart_view.setStyleSheet("background: transparent; border: none;")
        chart1_layout.addWidget(self.overall_chart_view)
        main_layout.addWidget(chart1_frame)
        
        # Voucher chart filter row
        filter_frame = QFrame()
        filter_frame.setObjectName("FilterFrame")
        self.filter_layout = QHBoxLayout(filter_frame)
        self.filter_layout.setContentsMargins(16, 10, 16, 10)
        self.filter_layout.setSpacing(12)
        
        filter_label = QLabel("表示設定:")
        filter_label.setObjectName("FilterLabel")
        self.filter_layout.addWidget(filter_label)
        
        self.voucher_metric_combo = QComboBox()
        for _, label, _, _ in METRIC_DEFS_ORDERED:
            self.voucher_metric_combo.addItem(label)
        self.voucher_metric_combo.currentIndexChanged.connect(self._on_voucher_metric_changed)
        self.filter_layout.addWidget(self.voucher_metric_combo)
        
        sep = QLabel("|")
        sep.setObjectName("FilterLabel")
        self.filter_layout.addWidget(sep)
        
        self._checkbox_area_anchor_index = self.filter_layout.count()
        self.filter_layout.addStretch()
        main_layout.addWidget(filter_frame)
        
        # Chart 2: By Voucher
        chart2_frame = QFrame()
        chart2_frame.setObjectName("ChartFrame")
        chart2_layout = QVBoxLayout(chart2_frame)
        chart2_layout.setContentsMargins(0, 0, 0, 0)
        
        self.voucher_chart = create_theme_chart(" 証憑別の推移 (複数線)", self._is_dark)
        self.voucher_chart_view = QChartView(self.voucher_chart)
        self.voucher_chart_view.setRenderHint(QPainter.Antialiasing)
        self.voucher_chart_view.setMinimumHeight(380)
        self.voucher_chart_view.setStyleSheet("background: transparent; border: none;")
        chart2_layout.addWidget(self.voucher_chart_view)
        main_layout.addWidget(chart2_frame)
        
        # Chart 3 (NEW): By Voucher Type - all metrics
        vtype_chart_frame = QFrame()
        vtype_chart_frame.setObjectName("ChartFrame")
        vtype_chart_layout = QVBoxLayout(vtype_chart_frame)
        vtype_chart_layout.setContentsMargins(12, 8, 12, 0)
        
        vtype_ctrl = QHBoxLayout()
        vtype_ctrl.setSpacing(8)
        vtype_label = QLabel(" 証憑タイプ:")
        vtype_label.setObjectName("FilterLabel")
        vtype_ctrl.addWidget(vtype_label)
        
        self.vtype_metric_combo = QComboBox()
        self.vtype_metric_combo.currentIndexChanged.connect(self._on_vtype_metric_changed)
        vtype_ctrl.addWidget(self.vtype_metric_combo)
        vtype_ctrl.addStretch()

        self.vtype_metric_chart = create_theme_chart(" 証憑タイプ別 - 項目別の推移", self._is_dark)
        self.vtype_metric_chart_view = QChartView(self.vtype_metric_chart)
        self.vtype_metric_chart_view.setRenderHint(QPainter.Antialiasing)
        self.vtype_metric_chart_view.setMinimumHeight(400) # Increased to fits filters
        self.vtype_metric_chart_view.setStyleSheet("background: transparent; border: none;")
        
        # Metric Chart Filters (NEW)
        vtype_metric_filter_frame = QFrame()
        vtype_metric_filter_frame.setObjectName("FilterFrame")
        self.metric_filter_layout = QHBoxLayout(vtype_metric_filter_frame)
        self.metric_filter_layout.setContentsMargins(16, 8, 16, 8)
        self.metric_filter_layout.setSpacing(10)
        vtype_metric_filter_label = QLabel("項目表示:")
        vtype_metric_filter_label.setObjectName("FilterLabel")
        self.metric_filter_layout.addWidget(vtype_metric_filter_label)
        self.metric_filter_layout.addStretch()
        
        vtype_chart_layout.addLayout(vtype_ctrl)
        vtype_chart_layout.addWidget(vtype_metric_filter_frame)
        vtype_chart_layout.addWidget(self.vtype_metric_chart_view)
        main_layout.addWidget(vtype_chart_frame)
        
        # Chart 4: Per-client trend (hidden by default)
        self.client_frame = QFrame()
        self.client_frame.setObjectName("ChartFrame")
        self.client_frame.setVisible(False)
        client_layout = QVBoxLayout(self.client_frame)
        client_layout.setContentsMargins(12, 8, 12, 0)
        
        client_header = QHBoxLayout()
        self.client_label = QLabel("")
        self.client_label.setObjectName("PageSubtitle")
        client_header.addWidget(self.client_label)
        client_header.addStretch()
        
        self.clear_client_btn = QPushButton("✕ クリア")
        self.clear_client_btn.setStyleSheet(
            "QPushButton { background-color: transparent; color: #EF4444; border: 1px solid #EF4444;"
            " padding: 4px 12px; border-radius: 4px; font-weight: 700; font-size: 10px; }"
            "QPushButton:hover { background-color: rgba(239, 68, 68, 0.1); }"
        )
        self.clear_client_btn.setCursor(Qt.PointingHandCursor)
        self.clear_client_btn.clicked.connect(self.clear_client_filter)
        client_header.addWidget(self.clear_client_btn)
        client_layout.addLayout(client_header)
        
        self.client_chart = create_theme_chart("", self._is_dark)
        self.client_chart_view = QChartView(self.client_chart)
        self.client_chart_view.setRenderHint(QPainter.Antialiasing)
        self.client_chart_view.setMinimumHeight(320)
        self.client_chart_view.setStyleSheet("background: transparent; border: none;")
        client_layout.addWidget(self.client_chart_view)
        main_layout.addWidget(self.client_frame)
        
        # Export button
        export_layout = QHBoxLayout()
        export_layout.addStretch()
        self.export_btn = QPushButton(" ダッシュボードデータをCSVエクスポート")
        self.export_btn.setStyleSheet(
            "QPushButton { background-color: #059669; color: white; padding: 8px 18px;"
            " border-radius: 6px; font-weight: 700; font-size: 11px; }"
            "QPushButton:hover { background-color: #047857; }"
        )
        self.export_btn.setCursor(Qt.PointingHandCursor)
        self.export_btn.clicked.connect(self._export_csv)
        export_layout.addWidget(self.export_btn)
        main_layout.addLayout(export_layout)
        
        # Install event filters to hide tooltip when mouse leaves any chart
        self.overall_chart_view.installEventFilter(self)
        self.voucher_chart_view.installEventFilter(self)
        self.vtype_metric_chart_view.installEventFilter(self)
        self.client_chart_view.installEventFilter(self)
        
        main_layout.addStretch()
        self.scroll.setWidget(container)
        
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(self.scroll)

    # ---- Helpers ----
    def _get_selected_metric(self, combo):
        idx = combo.currentIndex()
        if 0 <= idx < len(METRIC_DEFS_ORDERED):
            return METRIC_DEFS_ORDERED[idx]
        return METRIC_DEFS_ORDERED[0]

    def _get_month_range_filter(self):
        preset_idx = self.range_preset_combo.currentIndex()
        _, preset_val = RANGE_PRESETS[preset_idx]
        
        if preset_val is None:
            return ""
            
        if preset_val == -1:
            start = self.range_start_combo.currentText()
            end = self.range_end_combo.currentText()
            if start and end:
                return f'AND "処理月" >= \'{start}\' AND "処理月" <= \'{end}\''
            return ""
            
        if len(self._all_months) > 0:
            cutoff_months = self._all_months[-preset_val:] if len(self._all_months) >= preset_val else self._all_months
            if cutoff_months:
                return f'AND "処理月" >= \'{cutoff_months[0]}\''
        return ""

    def _populate_month_combos(self):
        try:
            data = self.engine.query(
                'SELECT DISTINCT "処理月" FROM master_data '
                'WHERE "処理月" IS NOT NULL ORDER BY "処理月"'
            )
            self._all_months = [str(r.get("処理月", "")) for r in data if r.get("処理月")]
        except Exception:
            self._all_months = []
            
        self.range_start_combo.blockSignals(True)
        self.range_end_combo.blockSignals(True)
        
        prev_start = self.range_start_combo.currentText()
        prev_end = self.range_end_combo.currentText()
        
        self.range_start_combo.clear()
        self.range_end_combo.clear()
        
        for m in self._all_months:
            self.range_start_combo.addItem(m)
            self.range_end_combo.addItem(m)
            
        if prev_start and prev_start in self._all_months:
            self.range_start_combo.setCurrentText(prev_start)
        elif self._all_months:
            self.range_start_combo.setCurrentIndex(0)
            
        if prev_end and prev_end in self._all_months:
            self.range_end_combo.setCurrentText(prev_end)
        elif self._all_months:
            self.range_end_combo.setCurrentIndex(len(self._all_months) - 1)
            
        self.range_start_combo.blockSignals(False)
        self.range_end_combo.blockSignals(False)

    def _refresh_present_voucher_types(self):
        try:
            data = self.engine.query(
                'SELECT DISTINCT "証憑タイプ" AS vt FROM master_data '
                'WHERE "証憑タイプ" IS NOT NULL AND "証憑タイプ" <> \'\''
            )
            self._present_vtypes = {str(r.get("vt", "")) for r in data if r.get("vt")}
        except Exception:
            self._present_vtypes = set()

    def _populate_vtype_combo(self):
        self.vtype_metric_combo.blockSignals(True)
        prev = self.vtype_metric_combo.currentData()
        self.vtype_metric_combo.clear()
        
        for code, label, _ in VOUCHER_MAP_ORDERED:
            if code in self._present_vtypes:
                self.vtype_metric_combo.addItem(label, code)
                
        known = {c for c, _, _ in VOUCHER_MAP_ORDERED}
        for code in sorted(self._present_vtypes - known):
            self.vtype_metric_combo.addItem(code, code)
            
        if prev is not None:
            for i in range(self.vtype_metric_combo.count()):
                if self.vtype_metric_combo.itemData(i) == prev:
                    self.vtype_metric_combo.setCurrentIndex(i)
                    break
        self.vtype_metric_combo.blockSignals(False)

    def _make_checkbox(self, code, label, color):
        cb = QCheckBox(label)
        cb.setChecked(True)
        cb.setStyleSheet(
            "QCheckBox { color: %s; font-size: 11px; font-weight: 600; spacing: 6px; }"
            "QCheckBox::indicator { width: 14px; height: 14px; border-radius: 3px; border: 1px solid %s; }"
            "QCheckBox::indicator:checked { background-color: %s; border-color: %s; }"
            % (
                "#CBD5E1" if self._is_dark else "#475569",
                "#475569" if self._is_dark else "#CBD5E1",
                color,
                color,
            )
        )
        cb.toggled.connect(self._on_filter_changed)
        return cb

    def _populate_checkboxes(self):
        # Remove checkboxes whose vtype is no longer present
        for code in list(self.checkboxes.keys()):
            if code not in self._present_vtypes:
                cb = self.checkboxes.pop(code)
                self.filter_layout.removeWidget(cb)
                cb.deleteLater()
                
        insert_idx = self._checkbox_area_anchor_index
        
        # Ordered known voucher types
        for code, label, color in VOUCHER_MAP_ORDERED:
            if code not in self._present_vtypes:
                continue
            if code not in self.checkboxes:
                cb = self._make_checkbox(code, label, color)
                self.checkboxes[code] = cb
                self.filter_layout.insertWidget(insert_idx, cb)
            else:
                self.filter_layout.insertWidget(insert_idx, self.checkboxes[code])
            insert_idx += 1
            
        # Unknown codes (present but not in ordered map)
        known = {c for c, _, _ in VOUCHER_MAP_ORDERED}
        for code in sorted(self._present_vtypes - known):
            if code in self.checkboxes:
                self.filter_layout.insertWidget(insert_idx, self.checkboxes[code])
                insert_idx += 1
                continue
            cb = self._make_checkbox(code, code, "#94A3B8")
            self.checkboxes[code] = cb
            self.filter_layout.insertWidget(insert_idx, cb)
            insert_idx += 1

    def _populate_metric_checkboxes(self):
        if self.metric_checkboxes:
            return # Already populated
            
        insert_idx = 1 # After label
        for idx, (code, label, _, _) in enumerate(METRIC_DEFS_ORDERED):
            color = METRIC_COLORS[idx % len(METRIC_COLORS)]
            cb = QCheckBox(label)
            cb.setChecked(True)
            cb.setStyleSheet(
                "QCheckBox { color: %s; font-size: 11px; font-weight: 600; spacing: 6px; }"
                "QCheckBox::indicator { width: 14px; height: 14px; border-radius: 3px; border: 1px solid %s; }"
                "QCheckBox::indicator:checked { background-color: %s; border-color: %s; }"
                % (
                    "#CBD5E1" if self._is_dark else "#475569",
                    "#475569" if self._is_dark else "#CBD5E1",
                    color,
                    color,
                )
            )
            cb.toggled.connect(self._on_metric_filter_changed)
            self.metric_checkboxes[code] = cb
            self.metric_filter_layout.insertWidget(insert_idx, cb)
            insert_idx += 1

    def _rescale_y_axis(self, chart, padding_ratio=0.08, min_padding=1.0,
                        floor=0.0, ceiling=100.0):
        """Recalculate Y-axis range based only on currently visible series."""
        visible_vals = []
        for s in chart.series():
            try:
                if not s.isVisible():
                    continue
            except Exception:
                continue
            for p in s.points():
                if p.y() > 0:
                    visible_vals.append(p.y())
                    
        if not visible_vals:
            return
            
        vmin = min(visible_vals)
        vmax = max(visible_vals)
        span = max(vmax - vmin, 0.0)
        pad = max(span * padding_ratio, min_padding)
        
        y_min = max(floor, vmin - pad)
        y_max = min(ceiling, vmax + pad)
        
        if y_max - y_min < 1.0:
            y_min = max(floor, y_min - 1.0)
            y_max = min(ceiling, y_max + 1.0)
            
        for ax in chart.axes(Qt.Vertical):
            ax.setRange(y_min, y_max)

    def hideEvent(self, event):
        if hasattr(self, "custom_tooltip"):
            self.custom_tooltip.hide()
        super().hideEvent(event)

    def eventFilter(self, watched, event):
        from PySide6.QtCore import QEvent
        if event.type() == QEvent.Leave:
            if hasattr(self, "custom_tooltip"):
                self.custom_tooltip.hide()
        return super().eventFilter(watched, event)

    # ---- Event Handlers ----
    def _on_range_preset_changed(self, index):
        _, val = RANGE_PRESETS[index]
        is_custom = (val == -1)
        self.range_start_label.setVisible(is_custom)
        self.range_start_combo.setVisible(is_custom)
        self.range_end_label.setVisible(is_custom)
        self.range_end_combo.setVisible(is_custom)
        self._rebuild_charts()

    def _on_range_changed(self):
        self._rebuild_charts()

    def _on_overall_metric_changed(self, index):
        self._build_overall_chart()

    def _on_voucher_metric_changed(self, index):
        self._build_voucher_chart()

    def _on_vtype_metric_changed(self, index):
        self._build_vtype_metric_chart()

    def _rebuild_charts(self):
        self._build_overall_chart()
        self._build_voucher_chart()
        self._build_vtype_metric_chart()
        if self._client_filter:
            self._build_client_chart(*self._client_filter)

    def set_theme(self, is_dark):
        self._is_dark = is_dark
        if hasattr(self, "custom_tooltip"):
            self.custom_tooltip.set_theme(is_dark)
            
        update_chart_theme(self.overall_chart, is_dark)
        update_chart_theme(self.voucher_chart, is_dark)
        update_chart_theme(self.vtype_metric_chart, is_dark)
        update_chart_theme(self.client_chart, is_dark)
        
        for code, cb in self.checkboxes.items():
            color = VOUCHER_MAP.get(code, ("", "#94A3B8"))[1]
            cb.setStyleSheet(
                "QCheckBox { color: %s; font-size: 11px; font-weight: 600; spacing: 6px; }"
                "QCheckBox::indicator { width: 14px; height: 14px; border-radius: 3px; border: 1px solid %s; }"
                "QCheckBox::indicator:checked { background-color: %s; border-color: %s; }"
                % (
                    "#CBD5E1" if is_dark else "#475569",
                    "#475569" if is_dark else "#CBD5E1",
                    color,
                    color,
                )
            )
            
        for m_code, cb in self.metric_checkboxes.items():
            # Get color from metric index
            try:
                idx = [m[0] for m in METRIC_DEFS_ORDERED].index(m_code)
                color = METRIC_COLORS[idx % len(METRIC_COLORS)]
            except:
                color = "#94A3B8"
            cb.setStyleSheet(
                "QCheckBox { color: %s; font-size: 11px; font-weight: 600; spacing: 6px; }"
                "QCheckBox::indicator { width: 14px; height: 14px; border-radius: 3px; border: 1px solid %s; }"
                "QCheckBox::indicator:checked { background-color: %s; border-color: %s; }"
                % (
                    "#CBD5E1" if is_dark else "#475569",
                    "#475569" if is_dark else "#CBD5E1",
                    color,
                    color,
                )
            )
            
        self._rebuild_charts()
        
        # Consistent background for the view and its scroll content
        bg = "#F8FAFC" if not is_dark else "#0F172A"
        self.setStyleSheet(f"QWidget#DashboardView {{ background-color: {bg}; }}")
        if hasattr(self, "scroll"):
            self.scroll.setStyleSheet(f"QScrollArea#DashboardScroll {{ background-color: {bg}; border: none; }}")
        
        container = self.findChild(QWidget, "DashboardContainer")
        if container:
            container.setStyleSheet(f"QWidget#DashboardContainer {{ background-color: {bg}; }}")

    # ---- Refresh ----
    def refresh(self):
        if not self.engine.has_data():
            return
        QTimer.singleShot(50, self._do_refresh)

    def _do_refresh(self):
        try:
            self._populate_month_combos()
            self._refresh_present_voucher_types()
            self._populate_checkboxes()
            self._populate_metric_checkboxes()
            self._populate_vtype_combo()
            self._update_stats()
            
            current_hash = self.engine.get_data_hash()
            if current_hash and current_hash == self._cache_hash:
                return
                
            self._build_overall_chart()
            self._build_voucher_chart()
            self._build_vtype_metric_chart()
            self._cache_hash = current_hash
            
            if self._client_filter:
                self._build_client_chart(*self._client_filter)
        except Exception as e:
            try:
                print(f"[DASHBOARD] Refresh error: {e}")
            except Exception:
                pass

    def _update_stats(self):
        required = ["クライアントID", "対象仕訳数", "全体正解件数"]
        try:
            cols = [c[0] for c in self.engine.conn.execute("SELECT * FROM master_data LIMIT 0").description]
            if not all(r in cols for r in required):
                print(f"[DASHBOARD] Schema mismatch. Missing: {[r for r in required if r not in cols]}")
                return
        except Exception:
            return

        data = self.engine.query(
            'SELECT COUNT(DISTINCT "クライアントID") as total_clients, '
            'SUM(TRY_CAST("対象仕訳数" AS INTEGER)) as total_vouchers, '
            'SUM(TRY_CAST("全体正解件数" AS INTEGER)) as total_correct, '
            'COUNT(*) as total_rows FROM master_data'
        )
        if not data:
            return
            
        row = data[0]
        clients = _safe_int(row.get("total_clients"))
        vouchers = _safe_int(row.get("total_vouchers"))
        correct = _safe_int(row.get("total_correct"))
        rows = _safe_int(row.get("total_rows"))
        
        acc = round(correct / vouchers * 100, 2) if vouchers > 0 else 0
        acc_trend = None
        
        try:
            trend_data = self.engine.query(
                'SELECT "処理月", '
                'SUM(TRY_CAST("対象仕訳数" AS INTEGER)) as t, '
                'SUM(TRY_CAST("全体正解件数" AS INTEGER)) as c '
                'FROM master_data WHERE "処理月" IS NOT NULL '
                'GROUP BY "処理月" ORDER BY "処理月" DESC LIMIT 2'
            )
            if len(trend_data) >= 2:
                t0 = _safe_int(trend_data[0].get("t"))
                c0 = _safe_int(trend_data[0].get("c"))
                t1 = _safe_int(trend_data[1].get("t"))
                c1 = _safe_int(trend_data[1].get("c"))
                if t0 > 0 and t1 > 0:
                    diff = (c0 / t0 * 100) - (c1 / t1 * 100)
                    acc_trend = f"{diff:+.1f}%"
        except Exception:
            pass
            
        self.card_clients.set_value(f"{clients:,}")
        self.card_accuracy.set_value(f"{acc}%", trend=acc_trend)
        self.card_vouchers.set_value(f"{vouchers:,}")
        self.card_rows.set_value(f"{rows:,}")
        
        # Update last sync time
        stats = self.engine.get_stats_summary()
        last_time = stats.get("last_load_time")
        if last_time:
            self.last_sync_label.setText(f"Last Sync: {last_time}")
        else:
            self.last_sync_label.setText("Last Sync: --")
            
        sync_prog = stats.get("sync_progress")
        if sync_prog:
            self.sync_progress_label.setText(f"Sync Progress: {sync_prog}")
        else:
            self.sync_progress_label.setText("Sync Progress: --")

    # ---- Chart 1: Overall ----
    def _build_overall_chart(self):
        _, metric_label, target_col, correct_col = self._get_selected_metric(self.overall_metric_combo)
        month_filter = self._get_month_range_filter()
        
        sql = (
            f'SELECT "処理月", '
            f'SUM(TRY_CAST("{target_col}" AS INTEGER)) as total_target, '
            f'SUM(TRY_CAST("{correct_col}" AS INTEGER)) as total_correct '
            f'FROM master_data WHERE "処理月" IS NOT NULL {month_filter} '
            f'GROUP BY "処理月" ORDER BY "処理月"'
        )
        data = self.engine.query(sql)
        if not data:
            return
            
        chart = self.overall_chart
        chart.removeAllSeries()
        for axis in chart.axes():
            chart.removeAxis(axis)
            
        chart.setTitle(f" 全社推移グラフ - {metric_label}")
        
        months = []
        rates = []
        for row in data:
            target = _safe_int(row.get("total_target"))
            correct = _safe_int(row.get("total_correct"))
            if target > 0:
                months.append(str(row.get("処理月", "")))
                rates.append(round(correct / target * 100, 2))
                
        if not months:
            return
            
        self._overall_months = months
        series = QLineSeries()
        series.setName(metric_label)
        series.setPointsVisible(True)
        
        pen = QPen(QColor("#818CF8"))
        pen.setWidth(3)
        series.setPen(pen)
        
        for i, rate in enumerate(rates):
            series.append(QPointF(float(i), rate))
            
        series.hovered.connect(lambda point, state: self._on_series_hovered(
            point, state, self.overall_chart_view, self._overall_months))
        series.clicked.connect(lambda point: self._on_series_clicked(
            point, self._overall_months))
            
        chart.addSeries(series)
        
        axis_x = QCategoryAxis()
        step = max(1, len(months) // 24)
        for i, m in enumerate(months):
            if i % step == 0 or i == len(months) - 1:
                axis_x.append(m, float(i))
        axis_x.setRange(-0.5, len(months) - 0.5)
        
        axis_color = QColor("#94A3B8") if self._is_dark else QColor("#475569")
        grid_color = QColor("#1E293B") if self._is_dark else QColor("#E2E8F0")
        line_color = QColor("#334155") if self._is_dark else QColor("#CBD5E1")
        
        axis_x.setLabelsColor(axis_color)
        axis_x.setLabelsFont(QFont("Segoe UI", 9))
        axis_x.setGridLineColor(grid_color)
        axis_x.setLinePenColor(line_color)
        
        axis_y = QValueAxis()
        axis_y.setLabelFormat("%.1f%%")
        axis_y.setLabelsColor(axis_color)
        axis_y.setLabelsFont(QFont("Segoe UI", 9))
        axis_y.setGridLineColor(grid_color)
        axis_y.setLinePenColor(line_color)
        axis_y.setTickCount(8)
        
        chart.addAxis(axis_x, Qt.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignLeft)
        series.attachAxis(axis_x)
        series.attachAxis(axis_y)
        
        self._rescale_y_axis(chart)

    # ---- Chart 2: By Voucher ----
    def _build_voucher_chart(self):
        _, metric_label, target_col, correct_col = self._get_selected_metric(self.voucher_metric_combo)
        month_filter = self._get_month_range_filter()
        
        sql = (
            f'SELECT "処理月", "証憑タイプ", '
            f'SUM(TRY_CAST("{target_col}" AS INTEGER)) as total_target, '
            f'SUM(TRY_CAST("{correct_col}" AS INTEGER)) as total_correct '
            f'FROM master_data '
            f'WHERE "処理月" IS NOT NULL AND "証憑タイプ" IS NOT NULL {month_filter} '
            f'GROUP BY "処理月", "証憑タイプ" ORDER BY "処理月"'
        )
        data = self.engine.query(sql)
        if not data:
            return
            
        chart = self.voucher_chart
        chart.removeAllSeries()
        self.series_map = {}
        
        for axis in chart.axes():
            chart.removeAxis(axis)
            
        chart.setTitle(f" 証憑別の推移 - {metric_label}")
        
        grouped = defaultdict(list)
        all_months = sorted(set(str(r.get("処理月", "")) for r in data))
        self._voucher_months = all_months
        
        for row in data:
            vtype = str(row.get("証憑タイプ", ""))
            month = str(row.get("処理月", ""))
            target = _safe_int(row.get("total_target"))
            correct = _safe_int(row.get("total_correct"))
            rate = round(correct / target * 100, 2) if target > 0 else 0
            grouped[vtype].append((month, rate))
            
        if not grouped:
            return
            
        series_list = []
        known = {c for c, _, _ in VOUCHER_MAP_ORDERED}
        
        for code, label, color in VOUCHER_MAP_ORDERED:
            if code not in grouped:
                continue
            entries = grouped[code]
            
            s = QLineSeries()
            s.setName(label)
            s.setPointsVisible(True)
            pen = QPen(QColor(color))
            pen.setWidth(2)
            s.setPen(pen)
            
            month_rate = {m: r for m, r in entries}
            for i, m in enumerate(all_months):
                rate = month_rate.get(m, 0)
                s.append(QPointF(float(i), rate))
                
            chart.addSeries(s)
            series_list.append(s)
            self.series_map[code] = s
            
            s.hovered.connect(lambda point, state, name=label: self._on_series_hovered(
                point, state, self.voucher_chart_view, self._voucher_months, name))
            s.clicked.connect(lambda point, code_val=code: self._on_series_clicked(
                point, self._voucher_months, vtype_code=code_val))
                
            if code in self.checkboxes:
                s.setVisible(self.checkboxes[code].isChecked())
                
        # Unknown vtypes
        for vtype, entries in grouped.items():
            if vtype in known:
                continue
            fallback_label, fallback_color = VOUCHER_MAP.get(vtype, (vtype, "#94A3B8"))
            
            s = QLineSeries()
            s.setName(fallback_label)
            s.setPointsVisible(True)
            pen = QPen(QColor(fallback_color))
            pen.setWidth(2)
            s.setPen(pen)
            
            month_rate = {m: r for m, r in entries}
            for i, m in enumerate(all_months):
                rate = month_rate.get(m, 0)
                s.append(QPointF(float(i), rate))
                
            chart.addSeries(s)
            series_list.append(s)
            self.series_map[vtype] = s
            
            s.hovered.connect(lambda point, state, name=fallback_label: self._on_series_hovered(
                point, state, self.voucher_chart_view, self._voucher_months, name))
            s.clicked.connect(lambda point, code_val=vtype: self._on_series_clicked(
                point, self._voucher_months, vtype_code=code_val))
                
            if vtype in self.checkboxes:
                s.setVisible(self.checkboxes[vtype].isChecked())
                
        axis_x = QCategoryAxis()
        step = max(1, len(all_months) // 24)
        for i, m in enumerate(all_months):
            if i % step == 0 or i == len(all_months) - 1:
                axis_x.append(m, float(i))
        axis_x.setRange(-0.5, len(all_months) - 0.5)
        
        axis_color = QColor("#94A3B8") if self._is_dark else QColor("#475569")
        grid_color = QColor("#1E293B") if self._is_dark else QColor("#E2E8F0")
        line_color = QColor("#334155") if self._is_dark else QColor("#CBD5E1")
        
        axis_x.setLabelsColor(axis_color)
        axis_x.setLabelsFont(QFont("Segoe UI", 9))
        axis_x.setGridLineColor(grid_color)
        axis_x.setLinePenColor(line_color)
        
        axis_y = QValueAxis()
        axis_y.setLabelFormat("%.1f%%")
        axis_y.setLabelsColor(axis_color)
        axis_y.setLabelsFont(QFont("Segoe UI", 9))
        axis_y.setGridLineColor(grid_color)
        axis_y.setLinePenColor(line_color)
        axis_y.setTickCount(10)
        
        chart.addAxis(axis_x, Qt.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignLeft)
        for s in series_list:
            s.attachAxis(axis_x)
            s.attachAxis(axis_y)
            
        self._rescale_y_axis(chart)

    def _on_filter_changed(self):
        for vtype, cb in self.checkboxes.items():
            if vtype in self.series_map:
                self.series_map[vtype].setVisible(cb.isChecked())
        self._rescale_y_axis(self.voucher_chart)

    def _on_metric_filter_changed(self):
        for metric_code, cb in self.metric_checkboxes.items():
            if metric_code in self.metric_series_map:
                self.metric_series_map[metric_code].setVisible(cb.isChecked())
        self._rescale_y_axis(self.vtype_metric_chart)

    # ---- Chart 3 (NEW): By Voucher Type, all metrics ----
    def _build_vtype_metric_chart(self):
        chart = self.vtype_metric_chart
        chart.removeAllSeries()
        self.metric_series_map = {}
        for axis in chart.axes():
            chart.removeAxis(axis)
            
        if self.vtype_metric_combo.count() == 0:
            chart.setTitle(" 証憑タイプ別 - 項目別の推移 (データなし)")
            return
            
        vtype_code = self.vtype_metric_combo.currentData()
        vtype_label = self.vtype_metric_combo.currentText()
        if not vtype_code:
            return
            
        month_filter = self._get_month_range_filter()
        agg_cols = []
        for _, _, target_col, correct_col in METRIC_DEFS_ORDERED:
            agg_cols.append(f'SUM(TRY_CAST("{target_col}" AS INTEGER)) AS "t_{target_col}"')
            agg_cols.append(f'SUM(TRY_CAST("{correct_col}" AS INTEGER)) AS "c_{correct_col}"')
            
        sql = (
            f'SELECT "処理月", {", ".join(agg_cols)} '
            f'FROM master_data '
            f'WHERE "証憑タイプ" = \'{vtype_code}\' AND "処理月" IS NOT NULL {month_filter} '
            f'GROUP BY "処理月" ORDER BY "処理月"'
        )
        data = self.engine.query(sql)
        
        if not data:
            chart.setTitle(f" {vtype_label} - 項目別の推移 (データなし)")
            return
            
        all_months = [str(r.get("処理月", "")) for r in data if r.get("処理月")]
        self._vtype_metric_months = all_months
        if not all_months:
            return
            
        chart.setTitle(f" {vtype_label} - 項目別の推移")
        series_list = []
        
        for idx, (m_code, metric_label, target_col, correct_col) in enumerate(METRIC_DEFS_ORDERED):
            color = METRIC_COLORS[idx % len(METRIC_COLORS)]
            s = QLineSeries()
            s.setName(metric_label)
            s.setPointsVisible(True)
            pen = QPen(QColor(color))
            pen.setWidth(2)
            s.setPen(pen)
            
            # Apply visibility from checkboxes
            if m_code in self.metric_checkboxes:
                s.setVisible(self.metric_checkboxes[m_code].isChecked())
            
            has_value = False
            for i, row in enumerate(data):
                t = _safe_int(row.get(f"t_{target_col}"))
                c = _safe_int(row.get(f"c_{correct_col}"))
                rate = round(c / t * 100, 2) if t > 0 else 0
                s.append(QPointF(float(i), rate))
                if rate > 0:
                    has_value = True
                    
            if not has_value:
                continue
                
            s.hovered.connect(lambda point, state, name=metric_label: self._on_series_hovered(
                point, state, self.vtype_metric_chart_view, self._vtype_metric_months, name))
            s.clicked.connect(lambda point, code_val=vtype_code: self._on_series_clicked(
                point, self._vtype_metric_months, vtype_code=code_val))
            chart.addSeries(s)
            series_list.append(s)
            self.metric_series_map[m_code] = s
            
        if not series_list:
            return
            
        axis_x = QCategoryAxis()
        step = max(1, len(all_months) // 24)
        for i, m in enumerate(all_months):
            if i % step == 0 or i == len(all_months) - 1:
                axis_x.append(m, float(i))
        axis_x.setRange(-0.5, len(all_months) - 0.5)
        
        axis_color = QColor("#94A3B8") if self._is_dark else QColor("#475569")
        grid_color = QColor("#1E293B") if self._is_dark else QColor("#E2E8F0")
        line_color = QColor("#334155") if self._is_dark else QColor("#CBD5E1")
        
        axis_x.setLabelsColor(axis_color)
        axis_x.setLabelsFont(QFont("Segoe UI", 9))
        axis_x.setGridLineColor(grid_color)
        axis_x.setLinePenColor(line_color)
        
        axis_y = QValueAxis()
        axis_y.setLabelFormat("%.1f%%")
        axis_y.setLabelsColor(axis_color)
        axis_y.setLabelsFont(QFont("Segoe UI", 9))
        axis_y.setGridLineColor(grid_color)
        axis_y.setLinePenColor(line_color)
        axis_y.setTickCount(8)
        
        chart.addAxis(axis_x, Qt.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignLeft)
        for s in series_list:
            s.attachAxis(axis_x)
            s.attachAxis(axis_y)
            
        self._rescale_y_axis(chart)

    # ---- Tooltip / Click handlers ----
    def _on_series_hovered(self, point, state, chart_view, months, series_name=None):
        if not self.isVisible():
            if hasattr(self, "custom_tooltip"):
                self.custom_tooltip.hide()
            return
            
        if state:
            idx = round(point.x())
            month_str = months[idx] if 0 <= idx < len(months) else "?"
            label = f"{series_name}: " if series_name else ""
            txt = f"{label}{month_str}\n{point.y():.2f}%"
            
            # Map point to global coordinates
            global_pos = chart_view.mapToGlobal(chart_view.chart().mapToPosition(point).toPoint())
            
            if hasattr(self, "custom_tooltip"):
                self.custom_tooltip.show_text(global_pos, txt)
        else:
            if hasattr(self, "custom_tooltip"):
                self.custom_tooltip.hide()

    def _on_series_clicked(self, point, months, vtype_code=None):
        if hasattr(self, "custom_tooltip"):
            self.custom_tooltip.hide()
            
        idx = round(point.x())
        if 0 <= idx < len(months):
            month_str = months[idx]
            filters = {"month": month_str}
            if vtype_code:
                filters["vtype"] = vtype_code
            if self._client_filter:
                filters["client_id"] = self._client_filter[0]
            self.point_clicked.emit(filters)

    # ---- Client Trend ----
    def show_client_trend(self, client_id, enterprise_name):
        self._client_filter = (client_id, enterprise_name)
        self.client_label.setText(f" {enterprise_name} ({client_id}) の推移")
        self.client_frame.setVisible(True)
        self._build_client_chart(client_id, enterprise_name)

    def clear_client_filter(self):
        self._client_filter = None
        self.client_frame.setVisible(False)

    def _build_client_chart(self, client_id, enterprise_name):
        self.client_chart = create_theme_chart(f" {enterprise_name} - 項目別推移", self._is_dark)
        chart = self.client_chart
        self.client_chart_view.setChart(chart)
        chart.removeAllSeries()
        
        for axis in chart.axes():
            chart.removeAxis(axis)
            
        month_filter = self._get_month_range_filter()
        all_months = set()
        metric_data = {}
        
        for _, metric_label, target_col, correct_col in METRIC_DEFS_ORDERED:
            sql = (
                f'SELECT "処理月", '
                f'SUM(TRY_CAST("{target_col}" AS INTEGER)) as total_target, '
                f'SUM(TRY_CAST("{correct_col}" AS INTEGER)) as total_correct '
                f'FROM master_data '
                f'WHERE "クライアントID" = \'{client_id}\' AND "処理月" IS NOT NULL {month_filter} '
                f'GROUP BY "処理月" ORDER BY "処理月"'
            )
            data = self.engine.query(sql)
            month_rate = {}
            for row in data:
                t = _safe_int(row.get("total_target"))
                c = _safe_int(row.get("total_correct"))
                m = str(row.get("処理月", ""))
                if t > 0:
                    month_rate[m] = round(c / t * 100, 2)
                    all_months.add(m)
            metric_data[metric_label] = month_rate
            
        sorted_months = sorted(all_months)
        self._client_months = sorted_months
        if not sorted_months:
            return
            
        series_list = []
        for idx, (_, metric_label, _, _) in enumerate(METRIC_DEFS_ORDERED):
            month_rate = metric_data.get(metric_label, {})
            if not any(month_rate.values()):
                continue
            color = METRIC_COLORS[idx % len(METRIC_COLORS)]
            s = QLineSeries()
            s.setName(metric_label)
            s.setPointsVisible(True)
            pen = QPen(QColor(color))
            pen.setWidth(2)
            s.setPen(pen)
            
            for i, m in enumerate(sorted_months):
                rate = month_rate.get(m, 0)
                s.append(QPointF(float(i), rate))
                
            s.hovered.connect(lambda point, state, name=metric_label: self._on_series_hovered(
                point, state, self.client_chart_view, sorted_months, name))
            s.clicked.connect(lambda point: self._on_series_clicked(
                point, sorted_months))
            chart.addSeries(s)
            series_list.append(s)
            
        if not series_list:
            return
            
        axis_x = QCategoryAxis()
        step = max(1, len(sorted_months) // 18)
        for i, m in enumerate(sorted_months):
            if i % step == 0 or i == len(sorted_months) - 1:
                axis_x.append(m, float(i))
        axis_x.setRange(-0.5, len(sorted_months) - 0.5)
        
        axis_color = QColor("#94A3B8") if self._is_dark else QColor("#475569")
        grid_color = QColor("#1E293B") if self._is_dark else QColor("#E2E8F0")
        line_color = QColor("#334155") if self._is_dark else QColor("#CBD5E1")
        
        axis_x.setLabelsColor(axis_color)
        axis_x.setLabelsFont(QFont("Segoe UI", 9))
        axis_x.setGridLineColor(grid_color)
        axis_x.setLinePenColor(line_color)
        
        axis_y = QValueAxis()
        axis_y.setLabelFormat("%.1f%%")
        axis_y.setLabelsColor(axis_color)
        axis_y.setLabelsFont(QFont("Segoe UI", 9))
        axis_y.setGridLineColor(grid_color)
        axis_y.setLinePenColor(line_color)
        axis_y.setTickCount(8)
        
        chart.addAxis(axis_x, Qt.AlignBottom)
        chart.addAxis(axis_y, Qt.AlignLeft)
        for s in series_list:
            s.attachAxis(axis_x)
            s.attachAxis(axis_y)
            
        self._rescale_y_axis(chart)

    # ---- Export ----
    def _export_csv(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "CSVエクスポート", "",
            "CSV files (*.csv);;All files (*.*)"
        )
        if not path:
            return
            
        try:
            month_filter = self._get_month_range_filter()
            sql = (
                'SELECT "処理月", "証憑タイプ", '
                'SUM(TRY_CAST("対象仕訳数" AS INTEGER)) as "対象仕訳数", '
                'SUM(TRY_CAST("全体正解件数" AS INTEGER)) as "全体正解件数" '
                f'FROM master_data WHERE "処理月" IS NOT NULL {month_filter} '
                'GROUP BY "処理月", "証憑タイプ" ORDER BY "処理月", "証憑タイプ"'
            )
            ok = self.engine.export_query_to_csv(sql, path)
            if ok:
                QMessageBox.information(self, "エクスポート完了", f"CSVファイルを保存しました:\n{path}")
            else:
                QMessageBox.warning(self, "エラー", "エクスポートに失敗しました。")
        except Exception as e:
            QMessageBox.warning(self, "エラー", f"エクスポートエラー: {e}")