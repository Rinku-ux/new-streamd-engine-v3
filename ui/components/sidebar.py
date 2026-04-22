from PySide6.QtWidgets import QWidget, QVBoxLayout, QPushButton, QLabel, QFrame
from PySide6.QtCore import Qt, Signal, QSize
from PySide6.QtGui import QIcon, QPixmap, QImage
import os


# SVG Icon data in Lucide style (Neutral: #94A3B8, Active: #818CF8)
from ui.components.icons import Icons

class SidebarItem(QPushButton):
    def __init__(self, item_id, title, icon_data, parent=None):
        super().__init__(parent)
        self.setObjectName(f"NavItem_{item_id}")
        self.item_id = item_id
        self.icon_data = icon_data
        self._title = title
        self.setText(f"      {title}")
        self.setCursor(Qt.PointingHandCursor)
        self.setProperty("active", False)
        self.setIconSize(QSize(20, 20))
        self._update_icon()

    def _update_icon(self):
        is_active = self.property("active")
        color = "#818CF8" if is_active else "#94A3B8"
        try:
            self.setIcon(Icons.get_icon(self.icon_data, 32, color))
        except Exception as e:
            print(f"Icon render error: {e}")

    def set_active(self, active):
        self.setProperty("active", active)
        self._update_icon()
        self.style().unpolish(self)
        self.style().polish(self)


class Sidebar(QWidget):
    nav_changed = Signal(str)
    theme_toggled = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("Sidebar")
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        # Title
        self.title = QLabel("Streamd BI")
        self.title.setObjectName("SidebarTitle")
        self.layout.addWidget(self.title)

        # Subtitle
        subtitle = QLabel("NATIVE ENGINE")
        subtitle.setObjectName("SidebarSubtitle")
        self.layout.addWidget(subtitle)

        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setObjectName("Separator")
        self.layout.addWidget(sep)

        # Nav Items
        self.items = {}
        self.add_nav_item("dashboard", "Dashboard", Icons.DASHBOARD)
        self.add_nav_item("ranking", "Data Table", Icons.TABLE) # Maps to NavItem_table in QSS
        self.add_nav_item("sync", "Sync", Icons.SYNC)
        self.add_nav_item("codemap", "Code Map", Icons.CODEMAP) # Maps to NavItem_book in QSS
        self.add_nav_item("settings", "Settings", Icons.SETTINGS)

        self.layout.addStretch()

        # Separator before footer
        sep2 = QFrame()
        sep2.setFrameShape(QFrame.HLine)
        sep2.setObjectName("Separator")
        self.layout.addWidget(sep2)

        # Theme Toggle
        self.theme_btn = QPushButton("      Dark Mode")
        self.theme_btn.setObjectName("NavItem_theme")
        self.theme_btn.setCursor(Qt.PointingHandCursor)
        self.theme_btn.setIconSize(QSize(20, 20))
        self.theme_btn.clicked.connect(self.theme_toggled.emit)
        self.layout.addWidget(self.theme_btn)

        # Version
        version_label = QLabel("v2.1.1")
        version_label.setObjectName("VersionLabel")
        version_label.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(version_label)

        self.layout.addSpacing(12)
        
        # Initial theme state for icons
        self.theme_btn.setProperty("theme", "dark")
        self._update_theme_icon(True)

    def add_nav_item(self, view_id, title, icon_data):
        # Map view_id to CSS ID as expected by styles.qss
        mapping = {
            "ranking": "table",
            "codemap": "book",
            "sync": "sync",
            "dashboard": "dashboard",
            "settings": "settings"
        }
        css_id = mapping.get(view_id, view_id)
        item = SidebarItem(css_id, title, icon_data)
        item.clicked.connect(lambda: self.on_item_clicked(view_id))
        self.layout.addWidget(item)
        self.items[view_id] = item

    def on_item_clicked(self, view_id):
        for vid, item in self.items.items():
            item.set_active(vid == view_id)
        self.nav_changed.emit(view_id)

    def set_active_item(self, view_id):
        self.on_item_clicked(view_id)

    def _update_theme_icon(self, is_dark):
        icon_data = Icons.MOON if is_dark else Icons.SUN
        color = "#94A3B8" if is_dark else "#FBBF24"
        try:
            self.theme_btn.setIcon(Icons.get_icon(icon_data, 32, color))
        except Exception as e:
            print(f"Theme icon error: {e}")

    def set_theme_label(self, is_dark):
        if is_dark:
            self.theme_btn.setText("      Dark Mode")
            self.theme_btn.setProperty("theme", "dark")
        else:
            self.theme_btn.setText("      Light Mode")
            self.theme_btn.setProperty("theme", "light")
        self._update_theme_icon(is_dark)
        self.theme_btn.style().unpolish(self.theme_btn)
        self.theme_btn.style().polish(self.theme_btn)
