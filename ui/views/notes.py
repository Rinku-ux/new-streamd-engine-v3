"""
Streamd BI - Notes & Comments Module
Local storage for annotations on data points.
"""
import os
import json
from datetime import datetime
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                               QFrame, QScrollArea, QPushButton, QTextEdit,
                               QComboBox, QTableWidget, QTableWidgetItem,
                               QDialog, QFormLayout, QLineEdit, QHeaderView,
                               QMessageBox)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QFont, QColor


CATEGORIES = ["メモ", "確認済み", "要調査", "対応完了", "重要"]
CATEGORY_COLORS = {
    "メモ": "#818CF8",
    "確認済み": "#10B981",
    "要調査": "#F59E0B",
    "対応完了": "#6366F1",
    "重要": "#EF4444",
}


class NoteDialog(QDialog):
    """Dialog for creating/editing a note."""
    def __init__(self, note=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle("ノート" if note is None else "ノート編集")
        self.setMinimumWidth(400)

        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.client_input = QLineEdit(note.get("client_id", "") if note else "")
        self.client_input.setPlaceholderText("クライアントID (任意)")
        form.addRow("クライアントID:", self.client_input)

        self.month_input = QLineEdit(note.get("month", "") if note else "")
        self.month_input.setPlaceholderText("処理月 (例: 2024-03)")
        form.addRow("処理月:", self.month_input)

        self.category_combo = QComboBox()
        self.category_combo.addItems(CATEGORIES)
        if note and note.get("category") in CATEGORIES:
            self.category_combo.setCurrentText(note["category"])
        form.addRow("カテゴリ:", self.category_combo)

        layout.addLayout(form)

        self.text_edit = QTextEdit()
        self.text_edit.setPlaceholderText("ノート内容を入力...")
        self.text_edit.setMinimumHeight(100)
        if note:
            self.text_edit.setPlainText(note.get("text", ""))
        layout.addWidget(self.text_edit)

        btn_layout = QHBoxLayout()
        btn_cancel = QPushButton("キャンセル")
        btn_cancel.clicked.connect(self.reject)
        btn_save = QPushButton("保存")
        btn_save.setObjectName("PrimaryBtn")
        btn_save.clicked.connect(self.accept)
        btn_layout.addStretch()
        btn_layout.addWidget(btn_cancel)
        btn_layout.addWidget(btn_save)
        layout.addLayout(btn_layout)

    def get_note_data(self):
        return {
            "client_id": self.client_input.text().strip(),
            "month": self.month_input.text().strip(),
            "category": self.category_combo.currentText(),
            "text": self.text_edit.toPlainText().strip(),
        }


class NotesManager:
    """Handles CRUD operations for notes stored in notes.json."""
    def __init__(self, base_dir):
        self.file_path = os.path.join(base_dir, "notes.json")
        self.notes = self._load()

    def _load(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return []
        return []

    def _save(self):
        with open(self.file_path, 'w', encoding='utf-8') as f:
            json.dump(self.notes, f, ensure_ascii=False, indent=2)

    def add(self, note_data):
        note = {
            "id": datetime.now().strftime("%Y%m%d%H%M%S%f"),
            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            **note_data,
        }
        self.notes.insert(0, note)
        self._save()
        return note

    def update(self, note_id, note_data):
        for note in self.notes:
            if note.get("id") == note_id:
                note.update(note_data)
                note["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                self._save()
                return True
        return False

    def delete(self, note_id):
        self.notes = [n for n in self.notes if n.get("id") != note_id]
        self._save()

    def filter(self, category=None, client_id=None):
        result = self.notes
        if category:
            result = [n for n in result if n.get("category") == category]
        if client_id:
            result = [n for n in result if n.get("client_id") == client_id]
        return result


class NotesView(QWidget):
    def __init__(self, base_dir, parent=None):
        super().__init__(parent)
        self.setObjectName("NotesView")
        self.manager = NotesManager(base_dir)
        self._is_dark = True

        layout = QVBoxLayout(self)
        layout.setContentsMargins(32, 24, 32, 32)
        layout.setSpacing(16)

        # Header
        header_row = QHBoxLayout()
        header_row.setSpacing(12)

        from ui.components.icons import Icons
        icon_label = QLabel()
        icon_label.setPixmap(Icons.get_pixmap(Icons.CODEMAP, 24, "#818CF8"))
        header_row.addWidget(icon_label)

        header = QLabel("Notes")
        header.setObjectName("PageHeader")
        header_row.addWidget(header)
        header_row.addStretch()

        add_btn = QPushButton("+ 新規ノート")
        add_btn.setObjectName("PrimaryBtn")
        add_btn.clicked.connect(self._add_note)
        header_row.addWidget(add_btn)
        layout.addLayout(header_row)

        # Filters
        filter_layout = QHBoxLayout()
        filter_label = QLabel("フィルター:")
        filter_label.setObjectName("FilterLabel")
        filter_layout.addWidget(filter_label)

        self.cat_filter = QComboBox()
        self.cat_filter.addItem("すべて", "")
        for cat in CATEGORIES:
            self.cat_filter.addItem(cat, cat)
        self.cat_filter.currentIndexChanged.connect(self._refresh_table)
        filter_layout.addWidget(self.cat_filter)
        filter_layout.addStretch()

        self.count_label = QLabel("")
        self.count_label.setStyleSheet("color: #94A3B8; font-size: 11px;")
        filter_layout.addWidget(self.count_label)
        layout.addLayout(filter_layout)

        # Notes Table
        self.table = QTableWidget()
        self.table.setColumnCount(6)
        self.table.setHorizontalHeaderLabels(["日時", "カテゴリ", "クライアントID", "処理月", "内容", "操作"])
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setDefaultSectionSize(40)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        layout.addWidget(self.table)

        self._refresh_table()

    def set_theme(self, is_dark):
        self._is_dark = is_dark
        bg = "#F8FAFC" if not is_dark else "#0F172A"
        self.setStyleSheet(f"QWidget#NotesView {{ background-color: {bg}; }}")

    def refresh(self):
        self._refresh_table()

    def _refresh_table(self):
        cat = self.cat_filter.currentData()
        notes = self.manager.filter(category=cat or None)

        self.table.setRowCount(len(notes))
        for row, note in enumerate(notes):
            # Date
            self.table.setItem(row, 0, QTableWidgetItem(note.get("date", "")))
            
            # Category with color
            cat_item = QTableWidgetItem(note.get("category", ""))
            cat_color = CATEGORY_COLORS.get(note.get("category", ""), "#94A3B8")
            cat_item.setForeground(QColor(cat_color))
            cat_item.setFont(QFont("Segoe UI", 10, QFont.Bold))
            self.table.setItem(row, 1, cat_item)
            
            self.table.setItem(row, 2, QTableWidgetItem(note.get("client_id", "")))
            self.table.setItem(row, 3, QTableWidgetItem(note.get("month", "")))

            # Text (truncated)
            text = note.get("text", "")
            display_text = text[:80] + "..." if len(text) > 80 else text
            self.table.setItem(row, 4, QTableWidgetItem(display_text))

            # Action buttons
            action_widget = QWidget()
            action_layout = QHBoxLayout(action_widget)
            action_layout.setContentsMargins(4, 2, 4, 2)
            action_layout.setSpacing(4)

            edit_btn = QPushButton("編集")
            edit_btn.setStyleSheet(
                "QPushButton { background: transparent; color: #818CF8; border: 1px solid #818CF8;"
                " padding: 2px 8px; border-radius: 3px; font-size: 10px; font-weight: 700; }"
            )
            note_id = note.get("id")
            edit_btn.clicked.connect(lambda checked, nid=note_id: self._edit_note(nid))

            del_btn = QPushButton("削除")
            del_btn.setStyleSheet(
                "QPushButton { background: transparent; color: #EF4444; border: 1px solid #EF4444;"
                " padding: 2px 8px; border-radius: 3px; font-size: 10px; font-weight: 700; }"
            )
            del_btn.clicked.connect(lambda checked, nid=note_id: self._delete_note(nid))

            action_layout.addWidget(edit_btn)
            action_layout.addWidget(del_btn)
            self.table.setCellWidget(row, 5, action_widget)

        self.count_label.setText(f"{len(notes)} 件")

    def _add_note(self):
        dialog = NoteDialog(parent=self)
        if dialog.exec() == QDialog.Accepted:
            data = dialog.get_note_data()
            if data.get("text"):
                self.manager.add(data)
                self._refresh_table()

    def _edit_note(self, note_id):
        note = next((n for n in self.manager.notes if n.get("id") == note_id), None)
        if not note:
            return
        dialog = NoteDialog(note=note, parent=self)
        if dialog.exec() == QDialog.Accepted:
            data = dialog.get_note_data()
            self.manager.update(note_id, data)
            self._refresh_table()

    def _delete_note(self, note_id):
        reply = QMessageBox.question(
            self, "確認", "このノートを削除しますか？",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply == QMessageBox.Yes:
            self.manager.delete(note_id)
            self._refresh_table()

    def add_note_for_client(self, client_id, month="", text=""):
        """External API: add a note pre-filled with context."""
        dialog = NoteDialog(
            note={"client_id": client_id, "month": month, "text": text},
            parent=self
        )
        if dialog.exec() == QDialog.Accepted:
            data = dialog.get_note_data()
            if data.get("text"):
                self.manager.add(data)
                self._refresh_table()
