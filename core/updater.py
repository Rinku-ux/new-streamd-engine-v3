import os
import sys
import json
import urllib.request
import tempfile
import zipfile
import shutil
import subprocess
from PySide6.QtCore import QObject, Signal

class UpdateManager(QObject):
    progress = Signal(int, str)
    finished = Signal(bool, str)
    
    CURRENT_VERSION = "2.1.3"
    # Production URL (version.json): https://drive.google.com/drive/folders/1JE4K1IW1RfAsDCOtfocvhEpoOZJS5KYV
    VERSION_URL = "https://drive.google.com/uc?export=download&id=1Od7B8qRGGA9tboIQ6djXyx0P6YzQ1cjs" 

    def __init__(self, parent=None):
        super().__init__(parent)
        self.tmp_zip = None

    def check_for_updates(self):
        """Checks if a newer version is available. Designed to be run in a thread."""
        print(f"[UPDATER] Checking URL: {self.VERSION_URL}")
        try:
            # Short timeout to avoid long hangs
            timeout = 10
            
            # Support local file for testing
            if os.path.exists(self.VERSION_URL):
                path = self.VERSION_URL
                print(f"[UPDATER] Reading local file: {path}")
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                import ssl
                # Create context to ignore SSL errors if needed on corporate networks (CAUTION)
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
                
                req = urllib.request.Request(self.VERSION_URL, headers={'User-Agent': 'Mozilla/5.0'})
                # Use system proxies
                with urllib.request.urlopen(req, timeout=timeout, context=ctx) as response:
                    content = response.read().decode('utf-8')
                    print(f"[UPDATER] Response received ({len(content)} bytes)")
                    data = json.loads(content)
            
            latest_version = data.get("version", "0.0.0")
            download_url = data.get("url", "")
            changelog = data.get("changelog", "")
            
            print(f"[UPDATER] Latest: {latest_version}, Current: {self.CURRENT_VERSION}")
            
            if self._is_newer(latest_version, self.CURRENT_VERSION):
                return True, latest_version, download_url, changelog
            return False, self.CURRENT_VERSION, "", ""
        except Exception as e:
            print(f"[UPDATER] Check failed: {e}")
            # Return error as changelog for display
            return False, self.CURRENT_VERSION, "", f"通信エラー: {str(e)}\n\n※企業内ネットワークの場合、プロキシやファイアウォールでブロックされている可能性があります。"

    def _is_newer(self, latest, current):
        try:
            l_parts = [int(p) for p in latest.split(".")]
            c_parts = [int(p) for p in current.split(".")]
            return l_parts > c_parts
        except:
            return latest > current

    def download_update(self, url):
        """Downloads the update ZIP to a temporary directory."""
        print(f"[UPDATER] Downloading from: {url}")
        try:
            self.progress.emit(0, "ダウンロードを開始しています...")
            
            # Support local file for testing
            if os.path.exists(url):
                print(f"[UPDATER] copying local file for test: {url}")
                self.tmp_zip = os.path.join(tempfile.gettempdir(), "streamdbi_update.zip")
                shutil.copy2(url, self.tmp_zip)
                self.progress.emit(100, "テスト用ローカルファイルのロード完了")
                return True

            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response:
                # Handle Google Drive large file virus warning wrapper
                info = response.info()
                print(f"[UPDATER] Headers: {info}")
                total_size = int(response.info().get('Content-Length', 0))
                downloaded = 0
                
                self.tmp_zip = os.path.join(tempfile.gettempdir(), "streamdbi_update.zip")
                with open(self.tmp_zip, 'wb') as f:
                    while True:
                        chunk = response.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = int(downloaded / total_size * 100)
                            self.progress.emit(percent, f"ダウンロード中... {percent}%")
            
            self.progress.emit(100, "ダウンロード完了")
            return True
        except Exception as e:
            self.finished.emit(False, f"ダウンロード失敗: {e}")
            return False

    def apply_update(self):
        """launches the external updater script and exits the app."""
        if not self.tmp_zip or not os.path.exists(self.tmp_zip):
            return False
            
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # Depending on if it's running as EXE or script
        if getattr(sys, 'frozen', False):
             install_dir = os.path.dirname(sys.executable)
        else:
             install_dir = base_dir

        # Create external bat script
        updater_bat = os.path.join(tempfile.gettempdir(), "streamdbi_apply.bat")
        
        # We need to escape paths for batch
        safe_zip = self.tmp_zip.replace('"', '')
        safe_dest = install_dir.replace('"', '')
        
        # Batch logic:
        # 1. Wait for process to exit
        # 2. Extract ZIP
        # 3. Restart app
        with open(updater_bat, "w", encoding="cp932") as f:
            f.write(f"@echo off\n")
            f.write(f"echo アプリの更新を適用しています...\n")
            f.write(f"timeout /t 3 /nobreak > nul\n")
            # Using powershell for extraction to avoid external dependencies like 7zip
            f.write(f"powershell -Command \"Expand-Archive -Path '{safe_zip}' -DestinationPath 'TEMP_EXTRACT' -Force\"\n")
            f.write(f"xcopy /y /s /e \"TEMP_EXTRACT\\*\" \"{safe_dest}\"\n")
            f.write(f"rd /s /q \"TEMP_EXTRACT\"\n")
            f.write(f"del \"{safe_zip}\"\n")
            f.write(f"start \"\" \"{sys.executable}\"\n")
            f.write(f"del \"%~f0\"\n")

        # Launch detached
        try:
            subprocess.Popen(["cmd.exe", "/c", updater_bat], 
                             creationflags=subprocess.CREATE_NEW_CONSOLE | subprocess.DETACHED_PROCESS)
            return True
        except Exception as e:
            print(f"[UPDATER] Launch failed: {e}")
            return False
