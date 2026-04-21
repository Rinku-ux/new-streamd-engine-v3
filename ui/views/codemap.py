from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                               QPushButton, QTableWidget, QTableWidgetItem,
                               QHeaderView, QTabWidget, QMessageBox)
from PySide6.QtCore import Qt

class CodeMapView(QWidget):
    def __init__(self, config, parent=None):
        super().__init__(parent)
        self.setObjectName("CodeMapView")
        self.config = config
        
        # Ensure code_map exists in saved data
        if "code_map" not in self.config.data:
            # Fallback to defaults from user request just in case
            default_map = {
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
                    "10": "(自動判定)", "20": "(自動判定)", "30": "(自動判定)"
                }
            }
            self.config.data["code_map"] = default_map

        layout = QVBoxLayout(self)
        layout.setContentsMargins(32, 24, 32, 24)
        layout.setSpacing(16)

        header = QLabel("📖 コードマップ (表示変換テーブル)")
        header.setObjectName("PageHeader")
        layout.addWidget(header)
        
        info = QLabel("ドリルダウン画面で数値コードを日本語に自動変換するための辞書です。")
        info.setObjectName("PageSubtitle")
        layout.addWidget(info)

        # Tab Widget
        self.tabs = QTabWidget()
        self.tabs.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #334155; border-radius: 8px; }
            QTabBar::tab { background: #1E293B; color: #94A3B8; padding: 10px 20px; border-top-left-radius: 6px; border-top-right-radius: 6px; border: 1px solid transparent; }
            QTabBar::tab:selected { background: #4F46E5; color: white; font-weight: bold; }
        """)
        layout.addWidget(self.tabs)

        self.tables = {}
        
        self.setup_tab("科目 (Account)", "account")
        self.setup_tab("税区分 (Tax)", "tax")

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        self.save_btn = QPushButton("💾 保存する")
        self.save_btn.setObjectName("PrimaryBtn")
        self.save_btn.clicked.connect(self.save_maps)
        btn_layout.addWidget(self.save_btn)

        layout.addLayout(btn_layout)
        
        self.load_maps()

    def setup_tab(self, label, key):
        tab = QWidget()
        vbox = QVBoxLayout(tab)
        vbox.setContentsMargins(16, 16, 16, 16)
        
        table = QTableWidget(0, 2)
        table.setHorizontalHeaderLabels(["元コード番号", "変換後の表示名"])
        table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        table.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        
        btn_layout = QHBoxLayout()
        add_btn = QPushButton("＋ 行を追加")
        add_btn.setObjectName("ActionBtn")
        add_btn.clicked.connect(lambda: self.add_row(table))
        
        del_btn = QPushButton("－ 選択行を削除")
        del_btn.setObjectName("DangerBtn")
        del_btn.clicked.connect(lambda: self.delete_row(table))
        
        btn_layout.addWidget(add_btn)
        btn_layout.addWidget(del_btn)
        btn_layout.addStretch()
        
        vbox.addLayout(btn_layout)
        vbox.addWidget(table)
        
        self.tables[key] = table
        self.tabs.addTab(tab, label)

    def load_maps(self):
        code_map = self.config.get("code_map", {})
        for key, table in self.tables.items():
            table.setRowCount(0)
            data = code_map.get(key, {})
            for code, name in data.items():
                self.add_row(table, code, name)

    def add_row(self, table, code="", name=""):
        row = table.rowCount()
        table.insertRow(row)
        table.setItem(row, 0, QTableWidgetItem(str(code)))
        table.setItem(row, 1, QTableWidgetItem(str(name)))

    def delete_row(self, table):
        for item in table.selectedItems():
            table.removeRow(item.row())

    def save_maps(self):
        code_map = {}
        for key, table in self.tables.items():
            map_data = {}
            for row in range(table.rowCount()):
                c_item = table.item(row, 0)
                n_item = table.item(row, 1)
                if c_item and n_item:
                    code = c_item.text().strip()
                    name = n_item.text().strip()
                    if code:
                        map_data[code] = name
            code_map[key] = map_data
        
        self.config.save({"code_map": code_map})
        QMessageBox.information(self, "保存完了", "コードマップを保存しました。ドリルダウン画面に反映されます。")
