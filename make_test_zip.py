import zipfile
import os

zip_path = r"c:\Users\Administrator\Desktop\new-streamd-engine-v2\test_update.zip"
content_path = r"c:\Users\Administrator\Desktop\new-streamd-engine-v2\test_update_content.txt"

with zipfile.ZipFile(zip_path, 'w') as zipf:
    zipf.write(content_path, "update_success.txt")

print(f"Created {zip_path}")
