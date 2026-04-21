import base64
import os

img_dir = 'docs/img'
for f in sorted(os.listdir(img_dir)):
    if f.endswith('.png'):
        path = os.path.join(img_dir, f)
        with open(path, 'rb') as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
            print(f"--- {f} ---")
            print(encoded_string)
            print("-" * 10)
