import json
import os

class Config:
    def __init__(self, config_path):
        self.config_path = config_path
        self.data = self._load()

    def _load(self):
        default_config = {
            "url": "",
            "key": "",
            "q993": "993",
            "q994": "994",
            "q1011": "1011",
            "start_date": "2024-01",
            "end_date": "2024-12",
            "voucher_type": "all",
            "item_filter": "overall",
            "threads": 5,
            "theme": "light",
            "code_map": {
                "account": {
                    "1": "未確定", "31": "現金", "32": "普通預金", "33": "当座預金", "37": "売掛金", "38": "仮払金", 
                    "26": "工具器具備品", "30": "事業主貸", "36": "買掛金", "39": "短期借入金", "34": "未払金", 
                    "35": "未払費用", "45": "仮受金", "40": "長期借入金", "41": "売上高", "25": "仕入高", 
                    "19": "給料賃金", "28": "法定福利費", "15": "福利厚生費", "20": "業務委託料", "6": "通信費", 
                    "11": "荷造運賃", "7": "水道光熱費", "2": "旅費交通費", "10": "広告宣伝費", "4": "接待交際費", 
                    "5": "会議費", "3": "備品・消耗品費", "9": "備品・消耗品費", "8": "新聞図書費", "17": "修繕費", 
                    "13": "地代家賃", "42": "車両費", "16": "保険料", "12": "租税公課", "29": "諸会費", 
                    "27": "リース料", "14": "支払手数料", "18": "減価償却費", "23": "雑費", "22": "貸倒金", 
                    "21": "支払利息", "43": "受取利息", "44": "雑収入", "46": "諸口", "24": "未確定勘定", 
                    "12055706": "預り金", "16702391": "AI Coding"
                },
                "tax": {
                    "5": "対象外", "12": "課税仕入 10%", "1": "課税仕入 8%", "14": "課税仕入 (軽)8%", 
                    "2": "対象外", "3": "非課税仕入", "22": "課税売上 10%", "6": "課税売上 8%", 
                    "24": "課税売上 (軽)8%", "7": "非課税売上", "32": "課税売上-貸倒 10%", 
                    "4": "課税売上-貸倒 8%", "34": "課税売上-貸倒 (軽)8%", 
                    "10": "(編集・削除不可)自動判定(仕入)", "20": "(編集・削除不可)自動判定(売上)", 
                    "30": "(編集・削除不可)自動判定(売上貸倒)"
                }
            }
        }
        
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if "code_map" not in data:
                        data["code_map"] = default_config["code_map"]
                    return data
            except:
                pass
        return default_config


    def reload(self):
        self.data = self._load()

    def save(self, data=None):
        if data:
            self.data.update(data)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def get(self, key, default=None):
        return self.data.get(key, default)
