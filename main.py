import sys
import os
import traceback

def crash_handler(exc_type, exc_value, exc_tb):
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'crash.log')
    with open(log_path, 'a', encoding='utf-8') as f:
        f.write('\n=== CRASH ===\n')
        traceback.print_exception(exc_type, exc_value, exc_tb, file=f)
    # Also try printing
    try:
        traceback.print_exception(exc_type, exc_value, exc_tb)
    except:
        pass

sys.excepthook = crash_handler

import threading
def thread_crash_handler(args):
    crash_handler(args.exc_type, args.exc_value, args.exc_traceback)
threading.excepthook = thread_crash_handler

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt, QTimer


from core.config import Config
from core.engine import DataEngine


def main():
    # Fix Windows console encoding for Japanese characters
    try:
        import sys
        if sys.platform == 'win32':
            sys.stdout.reconfigure(encoding='utf-8')
            sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Initialize Core (lightweight - no data loading yet)
    config_path = os.path.join(base_dir, 'config.json')
    config = Config(config_path)
    engine = DataEngine(base_dir)

    # Initialize UI
    app = QApplication(sys.argv)
    app.setStyle("Fusion")  # Force consistent cross-platform style to ignore OS theme interference
    
    # Apply Initial Stylesheet from Config
    theme = config.get("theme", "dark")
    
    # Force tooltip colors for Fusion style compatibility (especially on Windows)
    from PySide6.QtGui import QPalette, QColor
    palette = app.palette()
    if theme == "light":
        base = QColor("#FFFFFF")
        text = QColor("#1E293B")
    else:
        base = QColor("#1E293B")
        text = QColor("#F8FAFC")
        
    palette.setColor(QPalette.ToolTipBase, base)
    palette.setColor(QPalette.ToolTipText, text)
    palette.setColor(QPalette.Window, base)
    palette.setColor(QPalette.WindowText, text)
    app.setPalette(palette)

    if theme == "light":
        from ui.main_window import LIGHT_THEME
        app.setStyleSheet(LIGHT_THEME)
    else:
        style_path = os.path.join(base_dir, 'ui', 'styles.qss')
        if os.path.exists(style_path):
            with open(style_path, 'r', encoding='utf-8') as f:
                app.setStyleSheet(f.read())

    # Show window immediately, load data in background
    from ui.main_window import MainWindow
    window = MainWindow(config, engine)
    window.show()

    # Kick off background data load AFTER the window is visible
    QTimer.singleShot(100, window.load_data_async)

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
