from PySide6.QtWidgets import QWidget, QVBoxLayout, QLabel, QProgressBar, QFrame, QGraphicsDropShadowEffect
from PySide6.QtCore import Qt, Property, QPropertyAnimation, QEasingCurve, QTimer
from PySide6.QtGui import QColor, QFont

class ModernProgressOverlay(QWidget):
    """
    A premium, glassmorphism-inspired loading overlay for heavy operations.
    Displays a centered card with a progress bar and dynamic status updates.
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowFlags(Qt.FramelessWindowHint)
        self.setAttribute(Qt.WA_TranslucentBackground)
        
        # Main Background
        self._is_dark = True
        self.bg_color = QColor(15, 23, 42, 180) # Dark default
        
        # Main Layout
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setAlignment(Qt.AlignCenter)
        
        # Progress Card
        self.card = QFrame()
        self.card.setObjectName("ProgressCard")
        self.card.setFixedSize(450, 220)
        self.card.setStyleSheet("""
            QFrame#ProgressCard {
                background-color: #1E293B;
                border: 1px solid #334155;
                border-radius: 16px;
            }
        """)
        
        # Shadow Effect
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(30)
        shadow.setColor(QColor(0, 0, 0, 150))
        shadow.setOffset(0, 10)
        self.card.setGraphicsEffect(shadow)
        
        card_layout = QVBoxLayout(self.card)
        card_layout.setContentsMargins(35, 30, 35, 30)
        card_layout.setSpacing(15)
        
        # Header / Title
        self.title_label = QLabel("エクスポート中...")
        self.title_label.setStyleSheet("color: #F8FAFC; font-size: 18px; font-weight: 800;")
        self.title_label.setAlignment(Qt.AlignCenter)
        
        # Badge for R Engine
        self.badge = QLabel("R-ENGINE ACTIVE")
        self.badge.setFixedSize(140, 24)
        self.badge.setAlignment(Qt.AlignCenter)
        self.badge.setStyleSheet("""
            background-color: #4F46E5;
            color: white;
            border-radius: 12px;
            font-size: 10px;
            font-weight: 900;
            letter-spacing: 1px;
        """)
        
        # Progress Bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedHeight(8)
        self.progress_bar.setRange(0, 0) # Indeterminate by default
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                background-color: #334155;
                border-radius: 4px;
                border: none;
            }
            QProgressBar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #6366F1, stop:1 #818CF8);
                border-radius: 4px;
            }
        """)
        
        # Status Text
        self.status_label = QLabel("データを準備しています...")
        self.status_label.setStyleSheet("color: #94A3B8; font-size: 13px; font-weight: 600;")
        self.status_label.setAlignment(Qt.AlignCenter)
        
        # Footer
        footer_label = QLabel("High-Performance Streamd Engine")
        footer_label.setStyleSheet("color: #475569; font-size: 10px; font-weight: 700;")
        footer_label.setAlignment(Qt.AlignCenter)
        
        card_layout.addWidget(self.title_label)
        card_layout.addWidget(self.badge, 0, Qt.AlignCenter)
        card_layout.addSpacing(10)
        card_layout.addWidget(self.progress_bar)
        card_layout.addWidget(self.status_label)
        card_layout.addStretch()
        card_layout.addWidget(footer_label)
        
        self.main_layout.addWidget(self.card)
        
        # Hide by default
        self.hide()
        
    def paintEvent(self, event):
        from PySide6.QtGui import QPainter
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.fillRect(self.rect(), self.bg_color)
        
    def show_with_status(self, status):
        self.status_label.setText(status)
        self.resize(self.parent().size())
        self.raise_()
        self.show()
        
    def update_status(self, status):
        # Specific keywords mapping for better UX
        msg = status
        if "Rを実行中" in status:
            msg = "R言語エンジンで高速処理を実行中..."
        elif "マッピング" in status:
            msg = "ベクトル化エンジンによる変換を適用中..."
        elif "書き出し中" in status:
            msg = "最適化されたCSVを保存しています..."
            
        self.status_label.setText(msg)
        
    def set_theme(self, is_dark):
        self._is_dark = is_dark
        if is_dark:
            self.bg_color = QColor(15, 23, 42, 180)
            self.card.setStyleSheet("""
                QFrame#ProgressCard {
                    background-color: #1E293B;
                    border: 1px solid #334155;
                    border-radius: 16px;
                }
            """)
            self.title_label.setStyleSheet("color: #F8FAFC; font-size: 18px; font-weight: 800;")
            self.progress_bar.setStyleSheet("""
                QProgressBar { background-color: #334155; border-radius: 4px; border: none; }
                QProgressBar::chunk { background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #6366F1, stop:1 #818CF8); border-radius: 4px; }
            """)
            self.status_label.setStyleSheet("color: #94A3B8; font-size: 13px; font-weight: 600;")
        else:
            self.bg_color = QColor(248, 250, 252, 180) # Slate-50 with transparency
            self.card.setStyleSheet("""
                QFrame#ProgressCard {
                    background-color: #FFFFFF;
                    border: 1px solid #E2E8F0;
                    border-radius: 16px;
                }
            """)
            self.title_label.setStyleSheet("color: #0F172A; font-size: 18px; font-weight: 800;")
            self.progress_bar.setStyleSheet("""
                QProgressBar { background-color: #E2E8F0; border-radius: 4px; border: none; }
                QProgressBar::chunk { background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 #4F46E5, stop:1 #818CF8); border-radius: 4px; }
            """)
            self.status_label.setStyleSheet("color: #64748B; font-size: 13px; font-weight: 600;")
        self.update()

    def resizeEvent(self, event):
        if self.parent():
            self.resize(self.parent().size())
        super().resizeEvent(event)
