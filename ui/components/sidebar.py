from PySide6.QtWidgets import QWidget, QVBoxLayout, QPushButton, QLabel, QFrame
from PySide6.QtCore import Qt, Signal, QSize
from PySide6.QtGui import QIcon, QPixmap, QImage
import os


# SVG Icon data in Lucide style (Neutral: #94A3B8, Active: #818CF8)
ICON_DATA = {
    "dashboard": ("""<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#94A3B8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>""",
                  """<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#818CF8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>"""),
    "table": ("""<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#94A3B8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3h18v18H3zM3 9h18M3 15h18M9 3v18M15 3v18"/></svg>""",
              """<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#818CF8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 3h18v18H3zM3 9h18M3 15h18M9 3v18M15 3v18"/></svg>"""),
    "sync": ("""<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#94A3B8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 16h5v5"/></svg>""",
             """<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#818CF8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 16h5v5"/></svg>"""),
    "book": ("""<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#94A3B8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>""",
             """<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#818CF8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>"""),
    "settings": ("""<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#94A3B8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg>""",
                 """<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#818CF8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg>"""),
}

THEME_ICONS = {
    "moon": """<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#94A3B8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3a6 6 0 0 0 9 9 9 9 0 1 1-9-9Z"/></svg>""",
    "sun": """<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#FBBF24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2"/><path d="M12 20v2"/><path d="M4.93 4.93l1.41 1.41"/><path d="M17.66 17.66l1.41 1.41"/><path d="M2 12h2"/><path d="M20 12h2"/><path d="M4.93 19.07l1.41-1.41"/><path d="M17.66 6.34l1.41-1.41"/></svg>""",
}


def render_svg_to_icon(svg_str, size=24):
    """Render SVG string to QIcon using QSvgRenderer (manual bypass for missing plugin)."""
    from PySide6.QtCore import QByteArray
    from PySide6.QtGui import QPixmap, QImage, QPainter
    from PySide6.QtSvg import QSvgRenderer
    
    renderer = QSvgRenderer(QByteArray(svg_str.encode()))
    img = QImage(size, size, QImage.Format_ARGB32)
    img.fill(0) # transparent
    painter = QPainter(img)
    renderer.render(painter)
    painter.end()
    return QIcon(QPixmap.fromImage(img))


class SidebarItem(QPushButton):
    def __init__(self, title, icon_name, parent=None):
        super().__init__(parent)
        self.setObjectName(f"NavItem_{icon_name}")
        self.icon_name = icon_name
        self._title = title
        self.setText(f"      {title}")
        self.setCursor(Qt.PointingHandCursor)
        self.setProperty("active", False)
        self.setIconSize(QSize(20, 20))
        self._update_icon()

    def _update_icon(self):
        is_active = self.property("active")
        if self.icon_name in ICON_DATA:
            svg_str = ICON_DATA[self.icon_name][1] if is_active else ICON_DATA[self.icon_name][0]
            try:
                self.setIcon(render_svg_to_icon(svg_str, 32))
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
        self.add_nav_item("dashboard", "ダッシュボード", "dashboard")
        self.add_nav_item("ranking", "データテーブル", "table")
        self.add_nav_item("sync", "同期", "sync")
        self.add_nav_item("codemap", "コードマップ", "book")
        self.add_nav_item("settings", "設定", "settings")

        self.layout.addStretch()

        # Separator before footer
        sep2 = QFrame()
        sep2.setFrameShape(QFrame.HLine)
        sep2.setObjectName("Separator")
        self.layout.addWidget(sep2)

        # Theme Toggle
        self.theme_btn = QPushButton("      ダークモード")
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

    def add_nav_item(self, view_id, title, icon_name):
        item = SidebarItem(title, icon_name)
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
        svg_str = THEME_ICONS["moon"] if is_dark else THEME_ICONS["sun"]
        try:
            self.theme_btn.setIcon(render_svg_to_icon(svg_str, 32))
        except Exception as e:
            print(f"Theme icon error: {e}")

    def set_theme_label(self, is_dark):
        if is_dark:
            self.theme_btn.setText("      ダークモード")
            self.theme_btn.setProperty("theme", "dark")
        else:
            self.theme_btn.setText("      ライトモード")
            self.theme_btn.setProperty("theme", "light")
        self._update_theme_icon(is_dark)
        self.theme_btn.style().unpolish(self.theme_btn)
        self.theme_btn.style().polish(self.theme_btn)
