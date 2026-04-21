import sys
import os
import time
from PySide6.QtWidgets import QApplication
from PySide6.QtCore import QTimer, Qt
from core.config import Config
from core.engine import DataEngine
from ui.main_window import MainWindow

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'config.json')
    config = Config(config_path)
    engine = DataEngine(base_dir)

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    # Force dark theme for consistent manual screenshots
    style_path = os.path.join(base_dir, 'ui', 'styles.qss')
    if os.path.exists(style_path):
        with open(style_path, 'r', encoding='utf-8') as f:
            app.setStyleSheet(f.read())

    window = MainWindow(config, engine)
    window.show()
    
    # Create img directory
    img_dir = os.path.join(base_dir, 'docs', 'img')
    os.makedirs(img_dir, exist_ok=True)

    def do_capture():
        print("Starting capture...")
        views = ["dashboard", "ranking", "sync", "codemap", "settings"]
        for view_id in views:
            print(f"Capturing {view_id}...")
            window.switch_view(view_id)
            # Process events and wait a bit for rendering
            for _ in range(10):
                QApplication.processEvents()
                time.sleep(0.1)
            
            pixmap = window.grab()
            save_path = os.path.join(img_dir, f"{view_id}.png")
            pixmap.save(save_path)
            print(f"Saved to {save_path}")
        
        print("All captures finished.")
        app.quit()

    def check_ready():
        if window._views_initialized:
            # Wait a few more seconds for the data to actually appear in charts/tables
            QTimer.singleShot(2000, do_capture)
        else:
            QTimer.singleShot(500, check_ready)

    # Start the data load
    QTimer.singleShot(100, window.load_data_async)
    # Start checking for readiness
    QTimer.singleShot(500, check_ready)

    sys.exit(app.exec())

if __name__ == "__main__":
    main()
