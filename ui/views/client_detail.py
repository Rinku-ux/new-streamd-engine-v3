"""
Streamd BI - Client Detail View
Comprehensive single-client dashboard with all metrics.
"""
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                               QFrame, QGridLayout, QComboBox, QScrollArea,
                               QTableView, QPushButton, QFileDialog, QMessageBox,
                               QLineEdit)
from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QFont, QPainter, QColor
from PySide6.QtCharts import QChart, QChartView, QLineSeries, QValueAxis, QCategoryAxis
import math


def _safe_float(val):
    if val is None:
        return 0.0
    try:
        v = float(val)
        return 0.0 if math.isnan(v) else v
    except (ValueError, TypeError):
        return 0.0


METRIC_COLORS = {
    "総合精度": "#818CF8",
    "日付精度": "#F472B6",
    "金額精度": "#FBBF24",
    "科目精度": "#34D399",
    "支払先精度": "#FB923C",
    "税区分精度": "#A78BFA",
    "登録精度": "#22D3EE",
    "内容精度": "#F87171",
}


class ClientDetailView(QWidget):
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.setObjectName("ClientDetailView")
        self.engine = engine
        self._is_dark = True
        self._client_list = []
        self._current_client_id = None
        self._clients_loaded = False
        self._loading = False

        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QFrame.NoFrame)

        container = QWidget()
        main_layout = QVBoxLayout(container)
        main_layout.setContentsMargins(32, 24, 32, 32)
        main_layout.setSpacing(20)

        # Header
        header_row = QHBoxLayout()
        header_row.setSpacing(12)

        from ui.components.icons import Icons
        icon_label = QLabel()
        icon_label.setPixmap(Icons.get_pixmap(Icons.CLIENT, 24, "#818CF8"))
        header_row.addWidget(icon_label)

        header = QLabel("Client Detail")
        header.setObjectName("PageHeader")
        header_row.addWidget(header)
        header_row.addStretch()
        main_layout.addLayout(header_row)

        # Client Selector
        sel_frame = QFrame()
        sel_frame.setObjectName("FilterFrame")
        sel_layout = QHBoxLayout(sel_frame)
        sel_layout.setContentsMargins(16, 10, 16, 10)
        sel_layout.setSpacing(12)

        sel_label = QLabel("クライアント:")
        sel_label.setObjectName("FilterLabel")
        sel_layout.addWidget(sel_label)

        self.client_combo = QComboBox()
        self.client_combo.setMinimumWidth(300)
        self.client_combo.setEditable(True)
        self.client_combo.setInsertPolicy(QComboBox.NoInsert)
        self.client_combo.currentIndexChanged.connect(self._on_client_selected)
        sel_layout.addWidget(self.client_combo)
        sel_layout.addStretch()

        self.export_btn = QPushButton("CSVエクスポート")
        self.export_btn.setObjectName("SuccessBtn")
        self.export_btn.clicked.connect(self._export_csv)
        sel_layout.addWidget(self.export_btn)
        main_layout.addWidget(sel_frame)

        # Status label
        self.status_label = QLabel("")
        self.status_label.setStyleSheet("color: #94A3B8; font-size: 11px;")
        main_layout.addWidget(self.status_label)

        # Summary Cards
        cards_grid = QGridLayout()
        cards_grid.setSpacing(16)

        self.card_name = self._make_card("ENTERPRISE", "—", "#818CF8")
        self.card_months = self._make_card("MONTHS", "—", "#10B981")
        self.card_vouchers = self._make_card("TOTAL VOUCHERS", "—", "#F59E0B")
        self.card_accuracy = self._make_card("OVERALL ACCURACY", "—", "#EC4899")

        cards_grid.addWidget(self.card_name, 0, 0)
        cards_grid.addWidget(self.card_months, 0, 1)
        cards_grid.addWidget(self.card_vouchers, 0, 2)
        cards_grid.addWidget(self.card_accuracy, 0, 3)
        main_layout.addLayout(cards_grid)

        # All-Metrics Chart
        chart_frame = QFrame()
        chart_frame.setObjectName("ChartFrame")
        chart_layout = QVBoxLayout(chart_frame)
        chart_layout.setContentsMargins(12, 8, 12, 0)

        self.chart = QChart()
        self.chart.setTitle("全メトリクス推移")
        self.chart.setAnimationOptions(QChart.NoAnimation)
        self._apply_chart_theme()

        self.chart_view = QChartView(self.chart)
        self.chart_view.setRenderHint(QPainter.Antialiasing)
        self.chart_view.setMinimumHeight(360)
        self.chart_view.setStyleSheet("background: transparent; border: none;")
        chart_layout.addWidget(self.chart_view)
        main_layout.addWidget(chart_frame)

        # Monthly Detail Table
        from ui.views.ranking import PandasModel
        monthly_label = QLabel("月別サマリ")
        monthly_label.setObjectName("DetailSubHeader")
        main_layout.addWidget(monthly_label)

        self.monthly_model = PandasModel()
        self.monthly_table = QTableView()
        self.monthly_table.setModel(self.monthly_model)
        self.monthly_table.setAlternatingRowColors(True)
        self.monthly_table.verticalHeader().setVisible(False)
        self.monthly_table.setMinimumHeight(200)
        main_layout.addWidget(self.monthly_table)

        # Drilldown Table
        dd_label = QLabel("ドリルダウン詳細 (修正箇所)")
        dd_label.setObjectName("DetailSubHeader")
        main_layout.addWidget(dd_label)

        self.dd_model = PandasModel()
        self.dd_table = QTableView()
        self.dd_table.setModel(self.dd_model)
        self.dd_table.setAlternatingRowColors(True)
        self.dd_table.verticalHeader().setVisible(False)
        self.dd_table.setMinimumHeight(200)
        main_layout.addWidget(self.dd_table)

        main_layout.addStretch()
        self.scroll.setWidget(container)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(self.scroll)

    def _make_card(self, title, value, color):
        card = QFrame()
        card.setObjectName("StatCard")
        card.setMinimumHeight(100)
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(4)
        lbl = QLabel(title)
        lbl.setObjectName("StatCardTitle")
        layout.addWidget(lbl)
        val_lbl = QLabel(value)
        val_lbl.setStyleSheet(f"font-size: 22px; font-weight: 800; color: {color};")
        layout.addWidget(val_lbl)
        card._val_label = val_lbl
        return card

    def _apply_chart_theme(self):
        from PySide6.QtCore import QMargins
        bg = QColor("#0F172A") if self._is_dark else QColor("#FFFFFF")
        text = QColor("#F8FAFC") if self._is_dark else QColor("#1E293B")
        self.chart.setBackgroundBrush(bg)
        self.chart.setTitleBrush(text)
        self.chart.setTitleFont(QFont("Segoe UI", 14, QFont.Bold))
        legend = self.chart.legend()
        legend.setLabelColor(QColor("#CBD5E1") if self._is_dark else QColor("#475569"))
        legend.setFont(QFont("Segoe UI", 9))
        legend.setAlignment(Qt.AlignBottom)
        self.chart.setMargins(QMargins(12, 8, 12, 8))

    def set_theme(self, is_dark):
        self._is_dark = is_dark
        bg = "#F8FAFC" if not is_dark else "#0F172A"
        self.setStyleSheet(f"QWidget#ClientDetailView {{ background-color: {bg}; }}")
        self._apply_chart_theme()

    def refresh(self):
        if not self.engine.has_data():
            return
        # Only populate client list on first call
        if not self._clients_loaded:
            self._populate_clients()
            self._clients_loaded = True

    def _populate_clients(self):
        try:
            data = self.engine.query(
                'SELECT DISTINCT "クライアントID", "企業名" FROM master_data '
                'WHERE "クライアントID" IS NOT NULL ORDER BY "企業名"'
            )
            self._client_list = data
            prev_cid = self._current_client_id
            self.client_combo.blockSignals(True)
            self.client_combo.clear()
            for row in data:
                cid = str(row.get("クライアントID", ""))
                name = str(row.get("企業名", ""))
                label = f"{name} ({cid})" if name else cid
                self.client_combo.addItem(label, cid)
            if prev_cid:
                for i in range(self.client_combo.count()):
                    if self.client_combo.itemData(i) == prev_cid:
                        self.client_combo.setCurrentIndex(i)
                        break
            self.client_combo.blockSignals(False)
        except Exception:
            pass

    def _on_client_selected(self, idx):
        if idx < 0:
            return
        client_id = self.client_combo.itemData(idx)
        if not client_id:
            return
        self._current_client_id = client_id
        QTimer.singleShot(50, lambda: self._load_detail(client_id))

    def show_client(self, client_id):
        """External API to navigate to a specific client."""
        self._current_client_id = str(client_id)
        # Populate if needed
        if self.client_combo.count() == 0 and self.engine.has_data():
            self._populate_clients()
        # Select in combo
        for i in range(self.client_combo.count()):
            if self.client_combo.itemData(i) == str(client_id):
                self.client_combo.blockSignals(True)
                self.client_combo.setCurrentIndex(i)
                self.client_combo.blockSignals(False)
                break
        QTimer.singleShot(100, lambda: self._load_detail(str(client_id)))

    def _load_detail(self, client_id):
        if self._loading:
            return
        self._loading = True
        self.status_label.setText(f"読み込み中... ({client_id})")
        try:
            detail = self.engine.get_client_detail(client_id)
            summary = detail.get("summary", {})
            monthly = detail.get("monthly")
            dd = detail.get("drilldown")

            # Update cards
            ename = summary.get("enterprise_name", "—")
            if not ename or ename == "None":
                ename = "—"
            self.card_name._val_label.setText(str(ename))
            self.card_months._val_label.setText(str(summary.get("month_count", 0)))
            vouchers = summary.get("total_vouchers", 0)
            self.card_vouchers._val_label.setText(f"{int(vouchers or 0):,}")
            acc = summary.get("overall_accuracy", 0)
            self.card_accuracy._val_label.setText(f"{_safe_float(acc):.1f}%")

            # Monthly table
            if monthly is not None and not monthly.empty:
                self.monthly_model.set_dataframe(monthly)
                self.monthly_table.resizeColumnsToContents()
                self._build_chart(monthly)
                self.status_label.setText(
                    f"{ename} - {len(monthly)} レコード"
                )
            else:
                self.status_label.setText(f"{ename} - 月別データなし")

            # Drilldown table
            if dd is not None and not dd.empty:
                self.dd_model.set_dataframe(dd)
                self.dd_table.resizeColumnsToContents()
        except Exception as e:
            self.status_label.setText(f"エラー: {e}")
        finally:
            self._loading = False

    def _build_chart(self, df):
        from PySide6.QtGui import QPen
        self.chart.removeAllSeries()
        for ax in self.chart.axes():
            self.chart.removeAxis(ax)

        if df.empty:
            return

        # Aggregate by month: average across all voucher types for this client
        metric_cols = [c for c in df.columns if "精度" in c]
        if not metric_cols:
            return

        # Group by month, mean of each metric
        month_col = "処理月"
        if month_col not in df.columns:
            return

        monthly_avg = df.groupby(month_col)[metric_cols].mean()
        monthly_avg = monthly_avg.sort_index()
        months = monthly_avg.index.tolist()

        if not months:
            return

        # X axis
        x_axis = QCategoryAxis()
        x_axis.setLabelsAngle(-45)
        for i, m in enumerate(months):
            x_axis.append(str(m), i)
        x_axis.setRange(-0.5, len(months) - 0.5)

        # Y axis
        y_axis = QValueAxis()
        y_axis.setRange(0, 105)
        y_axis.setLabelFormat("%.0f%%")

        self.chart.addAxis(x_axis, Qt.AlignBottom)
        self.chart.addAxis(y_axis, Qt.AlignLeft)

        # Theme colors for axes
        lbl_color = QColor("#94A3B8") if self._is_dark else QColor("#475569")
        grid_color = QColor("#1E293B") if self._is_dark else QColor("#E2E8F0")
        for ax in [x_axis, y_axis]:
            ax.setLabelsColor(lbl_color)
            ax.setGridLineColor(grid_color)

        for col_name in metric_cols:
            color_str = METRIC_COLORS.get(col_name, "#94A3B8")
            series = QLineSeries()
            series.setName(col_name)
            from PySide6.QtGui import QPen
            pen = QPen(QColor(color_str))
            pen.setWidth(2)
            series.setPen(pen)

            for i, m in enumerate(months):
                val = _safe_float(monthly_avg.loc[m, col_name])
                if val > 0:
                    series.append(i, val)

            self.chart.addSeries(series)
            series.attachAxis(x_axis)
            series.attachAxis(y_axis)

    def _export_csv(self):
        client_id = self._current_client_id or self.client_combo.currentData()
        if not client_id:
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "CSVエクスポート", f"client_{client_id}_detail.csv",
            "CSV files (*.csv)"
        )
        if not path:
            return
        try:
            detail = self.engine.get_client_detail(client_id)
            monthly = detail.get("monthly")
            if monthly is not None and not monthly.empty:
                monthly.to_csv(path, index=False, encoding="utf-8-sig")
                QMessageBox.information(self, "完了", f"エクスポートが完了しました:\n{path}")
        except Exception as e:
            QMessageBox.warning(self, "エラー", f"エクスポートエラー: {e}")
