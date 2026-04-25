"""
Streamd BI - Comparison Dashboard View
Side-by-side comparison of two time periods.
"""
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                               QFrame, QGridLayout, QComboBox, QScrollArea)
from PySide6.QtCore import Qt, QTimer
from PySide6.QtGui import QFont


class ComparisonStatCard(QFrame):
    """A stat card that shows two values side by side with delta."""
    def __init__(self, title, parent=None):
        super().__init__(parent)
        self.setObjectName("StatCard")
        self.setMinimumHeight(130)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(4)

        lbl = QLabel(title)
        lbl.setObjectName("StatCardTitle")
        lbl.setAlignment(Qt.AlignCenter)
        layout.addWidget(lbl)

        vals_row = QHBoxLayout()
        vals_row.setSpacing(8)

        # Period A column
        col_a = QVBoxLayout()
        lbl_a = QLabel("期間 A")
        lbl_a.setStyleSheet("color: #818CF8; font-size: 11px; font-weight: bold;")
        lbl_a.setAlignment(Qt.AlignCenter)
        col_a.addWidget(lbl_a)
        
        self.val_a = QLabel("—")
        self.val_a.setStyleSheet("font-size: 24px; font-weight: 800; color: #818CF8;")
        self.val_a.setAlignment(Qt.AlignCenter)
        col_a.addWidget(self.val_a)
        vals_row.addLayout(col_a)

        # Arrow
        vs_label = QLabel("▶")
        vs_label.setStyleSheet("font-size: 18px; color: #475569; font-weight: 700;")
        vs_label.setAlignment(Qt.AlignCenter)
        vals_row.addWidget(vs_label)

        # Period B column
        col_b = QVBoxLayout()
        lbl_b = QLabel("期間 B")
        lbl_b.setStyleSheet("color: #10B981; font-size: 11px; font-weight: bold;")
        lbl_b.setAlignment(Qt.AlignCenter)
        col_b.addWidget(lbl_b)
        
        self.val_b = QLabel("—")
        self.val_b.setStyleSheet("font-size: 24px; font-weight: 800; color: #10B981;")
        self.val_b.setAlignment(Qt.AlignCenter)
        col_b.addWidget(self.val_b)
        vals_row.addLayout(col_b)

        layout.addLayout(vals_row)

        self.delta_label = QLabel("")
        self.delta_label.setAlignment(Qt.AlignCenter)
        self.delta_label.setStyleSheet("font-size: 14px; font-weight: 700;")
        layout.addWidget(self.delta_label)

    def set_values(self, a, b, fmt="{:,}", suffix=""):
        try:
            a_val = float(str(a).replace(",", "")) if a else 0
            b_val = float(str(b).replace(",", "")) if b else 0
            
            if fmt == "{:.1f}":
                self.val_a.setText(f"{a_val:.1f}{suffix}")
                self.val_b.setText(f"{b_val:.1f}{suffix}")
            else:
                self.val_a.setText(f"{int(a_val):,}{suffix}")
                self.val_b.setText(f"{int(b_val):,}{suffix}")

            delta = b_val - a_val
            if abs(delta) < 0.01:
                self.delta_label.setText("差分: — 変化なし")
                self.delta_label.setStyleSheet("font-size: 13px; font-weight: 700; color: #64748B;")
            elif delta > 0:
                self.delta_label.setText(f"差分: ▲ +{delta:.1f}{suffix}")
                self.delta_label.setStyleSheet("font-size: 13px; font-weight: 700; color: #10B981;")
            else:
                self.delta_label.setText(f"差分: ▼ {delta:.1f}{suffix}")
                self.delta_label.setStyleSheet("font-size: 13px; font-weight: 700; color: #EF4444;")
        except (ValueError, TypeError):
            self.val_a.setText(str(a or "—"))
            self.val_b.setText(str(b or "—"))
            self.delta_label.setText("")


class ComparisonView(QWidget):
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.setObjectName("ComparisonView")
        self.engine = engine
        self._is_dark = True
        self._all_months = []

        self.scroll = QScrollArea()
        self.scroll.setObjectName("ComparisonScroll")
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QFrame.NoFrame)

        container = QWidget()
        container.setObjectName("ComparisonContainer")
        main_layout = QVBoxLayout(container)
        main_layout.setContentsMargins(32, 24, 32, 32)
        main_layout.setSpacing(20)

        # Header
        header_row = QHBoxLayout()
        header_row.setSpacing(12)

        from ui.components.icons import Icons
        icon_label = QLabel()
        icon_label.setPixmap(Icons.get_pixmap(Icons.DASHBOARD, 24, "#818CF8"))
        header_row.addWidget(icon_label)

        header = QLabel("Period Comparison")
        header.setObjectName("PageHeader")
        header_row.addWidget(header)
        header_row.addStretch()
        main_layout.addLayout(header_row)

        subtitle = QLabel("2つの期間の精度・件数を並列比較")
        subtitle.setObjectName("PageSubtitle")
        main_layout.addWidget(subtitle)

        # Period Selectors
        sel_frame = QFrame()
        sel_frame.setObjectName("FilterFrame")
        sel_layout = QHBoxLayout(sel_frame)
        sel_layout.setContentsMargins(16, 12, 16, 12)
        sel_layout.setSpacing(12)

        # Period A
        a_label = QLabel("期間 A:")
        a_label.setObjectName("FilterLabel")
        a_label.setStyleSheet("color: #818CF8; font-weight: 700;")
        sel_layout.addWidget(a_label)

        self.a_start = QComboBox()
        self.a_end = QComboBox()
        sel_layout.addWidget(self.a_start)
        sel_layout.addWidget(QLabel("〜"))
        sel_layout.addWidget(self.a_end)

        sel_layout.addSpacing(24)

        # Period B
        b_label = QLabel("期間 B:")
        b_label.setObjectName("FilterLabel")
        b_label.setStyleSheet("color: #10B981; font-weight: 700;")
        sel_layout.addWidget(b_label)

        self.b_start = QComboBox()
        self.b_end = QComboBox()
        sel_layout.addWidget(self.b_start)
        sel_layout.addWidget(QLabel("〜"))
        sel_layout.addWidget(self.b_end)

        sel_layout.addStretch()

        from PySide6.QtWidgets import QPushButton
        self.compare_btn = QPushButton("比較実行")
        self.compare_btn.setObjectName("PrimaryBtn")
        self.compare_btn.clicked.connect(self._run_comparison)
        sel_layout.addWidget(self.compare_btn)

        main_layout.addWidget(sel_frame)

        # Presets row
        preset_layout = QHBoxLayout()
        preset_label = QLabel("プリセット:")
        preset_label.setObjectName("FilterLabel")
        preset_layout.addWidget(preset_label)

        presets = [
            ("前月比較", self._preset_last_month),
            ("前年同月比較", self._preset_yoy),
            ("四半期比較", self._preset_quarter),
        ]
        for label, func in presets:
            btn = QPushButton(label)
            btn.setStyleSheet(
                "QPushButton { background-color: transparent; border: 1px solid #475569;"
                " padding: 4px 12px; border-radius: 4px; font-weight: 600; font-size: 11px; color: #94A3B8; }"
                "QPushButton:hover { background-color: rgba(99, 102, 241, 0.1); color: #818CF8; }"
            )
            btn.setCursor(Qt.PointingHandCursor)
            btn.clicked.connect(func)
            preset_layout.addWidget(btn)
        preset_layout.addStretch()
        main_layout.addLayout(preset_layout)

        # Stat Cards
        cards_grid = QGridLayout()
        cards_grid.setSpacing(16)

        self.card_clients = ComparisonStatCard("クライアント数")
        self.card_vouchers = ComparisonStatCard("対象仕訳数")
        self.card_correct = ComparisonStatCard("正解仕訳数")
        self.card_accuracy = ComparisonStatCard("総合精度")

        cards_grid.addWidget(self.card_clients, 0, 0)
        cards_grid.addWidget(self.card_vouchers, 0, 1)
        cards_grid.addWidget(self.card_correct, 0, 2)
        cards_grid.addWidget(self.card_accuracy, 0, 3)
        main_layout.addLayout(cards_grid)

        # Result message
        self.result_label = QLabel("")
        self.result_label.setObjectName("PageSubtitle")
        self.result_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.result_label)

        main_layout.addStretch()
        self.scroll.setWidget(container)

        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(self.scroll)

    def set_theme(self, is_dark):
        self._is_dark = is_dark
        bg = "#F8FAFC" if not is_dark else "#0F172A"
        self.setStyleSheet(f"QWidget#ComparisonView {{ background-color: {bg}; }}")

    def refresh(self):
        if not self.engine.has_data():
            return
        self._populate_months()

    def _populate_months(self):
        try:
            data = self.engine.query(
                'SELECT DISTINCT "処理月" FROM master_data WHERE "処理月" IS NOT NULL ORDER BY "処理月"'
            )
            self._all_months = [str(r.get("処理月", "")) for r in data if r.get("処理月")]
        except Exception:
            self._all_months = []

        for combo in [self.a_start, self.a_end, self.b_start, self.b_end]:
            prev = combo.currentText()
            combo.blockSignals(True)
            combo.clear()
            for m in self._all_months:
                combo.addItem(m)
            if prev and prev in self._all_months:
                combo.setCurrentText(prev)
            combo.blockSignals(False)

        # Set reasonable defaults
        if len(self._all_months) >= 2:
            self.a_start.setCurrentIndex(max(0, len(self._all_months) - 2))
            self.a_end.setCurrentIndex(max(0, len(self._all_months) - 2))
            self.b_start.setCurrentIndex(len(self._all_months) - 1)
            self.b_end.setCurrentIndex(len(self._all_months) - 1)

    def _run_comparison(self):
        a_s = self.a_start.currentText()
        a_e = self.a_end.currentText()
        b_s = self.b_start.currentText()
        b_e = self.b_end.currentText()

        if not all([a_s, a_e, b_s, b_e]):
            self.result_label.setText("期間を選択してください。")
            return

        result = self.engine.get_comparison_data(a_s, a_e, b_s, b_e)
        a = result.get("a", {})
        b = result.get("b", {})

        self.card_clients.set_values(a.get("clients", 0), b.get("clients", 0))
        self.card_vouchers.set_values(a.get("total_vouchers", 0), b.get("total_vouchers", 0))
        self.card_correct.set_values(a.get("total_correct", 0), b.get("total_correct", 0))
        self.card_accuracy.set_values(
            a.get("accuracy", 0), b.get("accuracy", 0), fmt="{:.1f}", suffix="%"
        )

        self.result_label.setText(f"期間A ({a_s}〜{a_e}) vs 期間B ({b_s}〜{b_e})")

    # Presets
    def _preset_last_month(self):
        if len(self._all_months) >= 2:
            self.a_start.setCurrentIndex(len(self._all_months) - 2)
            self.a_end.setCurrentIndex(len(self._all_months) - 2)
            self.b_start.setCurrentIndex(len(self._all_months) - 1)
            self.b_end.setCurrentIndex(len(self._all_months) - 1)
            self._run_comparison()

    def _preset_yoy(self):
        if len(self._all_months) >= 13:
            self.a_start.setCurrentIndex(len(self._all_months) - 13)
            self.a_end.setCurrentIndex(len(self._all_months) - 13)
            self.b_start.setCurrentIndex(len(self._all_months) - 1)
            self.b_end.setCurrentIndex(len(self._all_months) - 1)
            self._run_comparison()

    def _preset_quarter(self):
        if len(self._all_months) >= 6:
            self.a_start.setCurrentIndex(len(self._all_months) - 6)
            self.a_end.setCurrentIndex(len(self._all_months) - 4)
            self.b_start.setCurrentIndex(len(self._all_months) - 3)
            self.b_end.setCurrentIndex(len(self._all_months) - 1)
            self._run_comparison()
