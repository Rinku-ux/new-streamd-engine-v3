import base64
import os
import json

img_dir = 'docs/img'
data = {}
for f in sorted(os.listdir(img_dir)):
    if f.endswith('.png'):
        path = os.path.join(img_dir, f)
        with open(path, 'rb') as image_file:
            data[f] = base64.b64encode(image_file.read()).decode('utf-8')

with open('b64.json', 'w') as f:
    json.dump(data, f)
