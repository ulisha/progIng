import sys
import os
import csv
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QLineEdit, QPushButton, QComboBox, QTableWidget, QTableWidgetItem,
                             QTabWidget, QFileDialog, QMessageBox, QTextEdit, QListWidget, QCheckBox,
                             QGroupBox, QHeaderView, QColorDialog, QStyledItemDelegate, QStyle, QSizePolicy)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QColor, QFont, QPalette
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, inspect, text, PrimaryKeyConstraint, ForeignKeyConstraint
from sqlalchemy.types import Integer, String, Float, DateTime, Date, Boolean
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import random
from faker import Faker


class ComboBoxDelegate(QStyledItemDelegate):
    def __init__(self, items, parent=None):
        super().__init__(parent)
        self.items = items

    def createEditor(self, parent, option, index):
        editor = QComboBox(parent)
        editor.addItems(self.items)
        return editor

    def setEditorData(self, editor, index):
        value = index.model().data(index, Qt.EditRole)
        if value:
            editor.setCurrentText(value)

    def setModelData(self, editor, model, index):
        model.setData(index, editor.currentText(), Qt.EditRole)


class DatabaseManager:
    def __init__(self):
        self.engine = None
        self.metadata = MetaData()

    def connect(self, db_type, host, port, database, username, password):
        try:
            if db_type == 'PostgreSQL':
                connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            elif db_type == 'MySQL':
                connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
            else:
                raise ValueError("Unsupported database type")

            self.engine = create_engine(connection_string)
            self.metadata.reflect(bind=self.engine)
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)

    def get_tables(self):
        if not self.engine:
            return []
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def get_table_columns(self, table_name):
        if not self.engine:
            return []
        inspector = inspect(self.engine)
        return inspector.get_columns(table_name)

    def get_foreign_keys(self, table_name):
        if not self.engine:
            return []
        inspector = inspect(self.engine)
        return inspector.get_foreign_keys(table_name)

    def get_primary_keys(self, table_name):
        if not self.engine:
            return []
        inspector = inspect(self.engine)
        return inspector.get_pk_constraint(table_name)['constrained_columns']

    def execute_sql(self, sql):
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(sql))
                if result.returns_rows:
                    return True, result.fetchall()
                else:
                    connection.commit()
                    return True, "Query executed successfully"
        except SQLAlchemyError as e:
            return False, str(e)

    def create_table(self, table_name, columns, constraints):
        try:
            # Создаем список объектов Column
            sqlalchemy_columns = []
            primary_key_cols = []

            for col in columns:
                # Создаем объект Column с правильным типом
                column = Column(
                    col['name'],
                    self._get_sqlalchemy_type(col['type']),
                    primary_key=col.get('primary_key', False),
                    nullable=col.get('nullable', True)
                )
                sqlalchemy_columns.append(column)

                if col.get('primary_key', False):
                    primary_key_cols.append(col['name'])

            # Подготавливаем аргументы таблицы
            table_args = []

            if primary_key_cols:
                table_args.append(PrimaryKeyConstraint(*primary_key_cols))

            for constraint in constraints:
                if constraint['type'] == 'foreign_key':
                    table_args.append(ForeignKeyConstraint(
                        [constraint['source_column']],
                        [f"{constraint['target_table']}.{constraint['target_column']}"]
                    ))

            # Создаем таблицу
            table = Table(table_name, self.metadata, *sqlalchemy_columns, *table_args)
            table.create(self.engine)
            return True, f"Table {table_name} created successfully"

        except SQLAlchemyError as e:
            return False, str(e)

        except SQLAlchemyError as e:
            return False, str(e)

    def add_column(self, table_name, column_name, column_type, nullable=True):
        try:
            # Получаем SQL-тип для указанного типа колонки
            sql_type = self._get_sql_type(column_type)

            with self.engine.connect() as connection:
                # Формируем SQL-запрос для добавления колонки
                sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}"
                if not nullable:
                    sql += " NOT NULL"

                # Выполняем запрос
                connection.execute(text(sql))
                connection.commit()

                return True, f"Column {column_name} added to {table_name}"

        except SQLAlchemyError as e:
            return False, str(e)

    def _get_sql_type(self, type_str):
        """Преобразует тип из интерфейса в SQL-тип"""
        type_mapping = {
            'Integer': 'INTEGER',
            'String': 'VARCHAR(255)',
            'Float': 'FLOAT',
            'DateTime': 'TIMESTAMP',
            'Date': 'DATE',
            'Boolean': 'BOOLEAN'
        }
        return type_mapping.get(type_str, 'VARCHAR(255)')

    def insert_data(self, table_name, data):
        try:
            with self.engine.connect() as connection:
                data.to_sql(table_name, connection, if_exists='append', index=False)
                return True, f"Data inserted into {table_name}"
        except SQLAlchemyError as e:
            return False, str(e)

    def truncate_table(self, table_name):
        try:
            with self.engine.connect() as connection:
                connection.execute(text(f"TRUNCATE TABLE {table_name}"))
                connection.commit()
                return True, f"Table {table_name} truncated successfully"
        except SQLAlchemyError as e:
            return False, str(e)

    def drop_table(self, table_name):
        try:
            with self.engine.connect() as connection:
                connection.execute(text(f"DROP TABLE {table_name}"))
                connection.commit()
                return True, f"Table {table_name} dropped successfully"
        except SQLAlchemyError as e:
            return False, str(e)

    def _get_sqlalchemy_type(self, type_str):
        type_mapping = {
            'Integer': Integer(),
            'String': String(255),  # Добавляем длину для VARCHAR
            'Float': Float(),
            'DateTime': DateTime(),
            'Date': Date(),
            'Boolean': Boolean()
        }
        return type_mapping.get(type_str, String(255))


class ETLApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Flow Manager")

        # Получаем размеры экрана
        screen_geometry = QApplication.primaryScreen().availableGeometry()
        max_width = screen_geometry.width() * 0.9  # 90% ширины экрана
        max_height = screen_geometry.height() * 0.9  # 90% высоты экрана

        # Устанавливаем начальный и максимальный размеры
        self.setMinimumSize(800, 600)
        self.setMaximumSize(int(max_width), int(max_height))

        # Центрируем окно
        center_point = screen_geometry.center()
        self.move(center_point - self.rect().center())
        # Set application style
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f0f0f0;
            }
            QTabWidget::pane {
                border: 1px solid #c4c4c4;
                background: white;
            }
            QTabBar::tab {
                background: #e0e0e0;
                border: 1px solid #c4c4c4;
                padding: 8px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
            }
            QTabBar::tab:selected {
                background: #4CAF50;
                color: white;
            }
            QPushButton {
                background-color: #4CAF50;
                color: white;
                border: none;
                padding: 8px 16px;
                text-align: center;
                text-decoration: none;
                font-size: 14px;
                margin: 4px 2px;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #45a049;
            }
            QPushButton:pressed {
                background-color: #3e8e41;
            }
            QLineEdit, QComboBox, QTextEdit {
                padding: 6px;
                border: 1px solid #ccc;
                border-radius: 4px;
            }
            QTableWidget {
                background-color: white;
                alternate-background-color: #f8f8f8;
                gridline-color: #ddd;
            }
            QHeaderView::section {
                background-color: #4CAF50;
                color: white;
                padding: 4px;
            }
            QListWidget {
                background-color: white;
                border: 1px solid #ccc;
                border-radius: 4px;
            }
            QGroupBox {
                border: 1px solid #ccc;
                border-radius: 4px;
                margin-top: 10px;
                padding-top: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 3px;
            }
        """)

        self.db_manager = DatabaseManager()
        self.source_data = None
        self.current_etl_flow = {}

        self.init_ui()
        self.generate_sample_csv_files()

    def init_ui(self):
        self.tabs = QTabWidget()
        self.tabs.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        # Connection Tab
        self.connection_tab = QWidget()
        self.init_connection_tab()
        self.tabs.addTab(self.connection_tab, "Database Connection")

        # Data Source Tab
        self.data_source_tab = QWidget()
        self.init_data_source_tab()
        self.tabs.addTab(self.data_source_tab, "Data Source")

        # Data Cleaning Tab
        self.data_cleaning_tab = QWidget()
        self.init_data_cleaning_tab()
        self.tabs.addTab(self.data_cleaning_tab, "Data Cleaning")

        # Table Creation Tab
        self.table_creation_tab = QWidget()
        self.init_table_creation_tab()
        self.tabs.addTab(self.table_creation_tab, "Table Creation")

        # Field Mapping Tab
        self.field_mapping_tab = QWidget()
        self.init_field_mapping_tab()
        self.tabs.addTab(self.field_mapping_tab, "Field Mapping")

        # Query Tab
        self.query_tab = QWidget()
        self.init_query_tab()
        self.tabs.addTab(self.query_tab, "Query Interface")

        self.setCentralWidget(self.tabs)


    def init_connection_tab(self):
        layout = QVBoxLayout()

        # Database Type
        db_type_layout = QHBoxLayout()
        db_type_layout.addWidget(QLabel("Database Type:"))
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["PostgreSQL", "MySQL"])
        db_type_layout.addWidget(self.db_type_combo)
        layout.addLayout(db_type_layout)

        # Host
        host_layout = QHBoxLayout()
        host_layout.addWidget(QLabel("Host:"))
        self.host_input = QLineEdit("localhost")
        host_layout.addWidget(self.host_input)
        layout.addLayout(host_layout)

        # Port
        port_layout = QHBoxLayout()
        port_layout.addWidget(QLabel("Port:"))
        self.port_input = QLineEdit("5432")
        port_layout.addWidget(self.port_input)
        layout.addLayout(port_layout)

        # Database
        db_layout = QHBoxLayout()
        db_layout.addWidget(QLabel("Database:"))
        self.db_input = QLineEdit()
        db_layout.addWidget(self.db_input)
        layout.addLayout(db_layout)

        # Username
        user_layout = QHBoxLayout()
        user_layout.addWidget(QLabel("Username:"))
        self.user_input = QLineEdit()
        user_layout.addWidget(self.user_input)
        layout.addLayout(user_layout)

        # Password
        pwd_layout = QHBoxLayout()
        pwd_layout.addWidget(QLabel("Password:"))
        self.pwd_input = QLineEdit()
        self.pwd_input.setEchoMode(QLineEdit.Password)
        pwd_layout.addWidget(self.pwd_input)
        layout.addLayout(pwd_layout)

        # Connect Button
        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self.connect_to_db)
        layout.addWidget(self.connect_btn)

        # Status
        self.connection_status = QLabel("Not connected")
        self.connection_status.setStyleSheet("font-weight: bold; color: #d32f2f;")
        layout.addWidget(self.connection_status)

        self.connection_tab.setLayout(layout)

    def init_data_source_tab(self):
        layout = QVBoxLayout()

        # Source Type
        source_type_layout = QHBoxLayout()
        source_type_layout.addWidget(QLabel("Source Type:"))
        self.source_type_combo = QComboBox()
        self.source_type_combo.addItems(["CSV File", "Database Table"])
        self.source_type_combo.currentTextChanged.connect(self.update_source_ui)
        source_type_layout.addWidget(self.source_type_combo)
        layout.addLayout(source_type_layout)

        # CSV Source UI
        self.csv_source_ui = QWidget()
        csv_layout = QVBoxLayout()
        self.csv_file_btn = QPushButton("Select CSV File")
        self.csv_file_btn.clicked.connect(self.load_csv_file)
        csv_layout.addWidget(self.csv_file_btn)
        self.csv_file_label = QLabel("No file selected")
        self.csv_file_label.setStyleSheet("color: #666;")
        csv_layout.addWidget(self.csv_file_label)
        self.csv_source_ui.setLayout(csv_layout)
        layout.addWidget(self.csv_source_ui)

        # Database Source UI
        self.db_source_ui = QWidget()
        db_source_layout = QVBoxLayout()

        # Tables Combo
        table_layout = QHBoxLayout()
        table_layout.addWidget(QLabel("Table:"))
        self.table_combo = QComboBox()
        table_layout.addWidget(self.table_combo)
        db_source_layout.addLayout(table_layout)

        # Load Button
        self.load_table_btn = QPushButton("Load Table Data")
        self.load_table_btn.clicked.connect(self.load_db_table)
        db_source_layout.addWidget(self.load_table_btn)

        self.db_source_ui.setLayout(db_source_layout)
        layout.addWidget(self.db_source_ui)
        self.db_source_ui.hide()

        # Data Preview
        self.data_preview = QTableWidget()
        self.data_preview.setAlternatingRowColors(True)
        self.data_preview.setStyleSheet("QTableWidget { font-size: 12px; }")
        layout.addWidget(QLabel("Data Preview:"))
        layout.addWidget(self.data_preview)

        self.data_source_tab.setLayout(layout)

    def init_data_cleaning_tab(self):
        layout = QVBoxLayout()

        # Remove Duplicates
        self.remove_duplicates_btn = QPushButton("Remove Duplicates")
        self.remove_duplicates_btn.clicked.connect(self.remove_duplicates)
        layout.addWidget(self.remove_duplicates_btn)

        # Data After Cleaning
        self.cleaned_data_preview = QTableWidget()
        self.cleaned_data_preview.setAlternatingRowColors(True)
        self.cleaned_data_preview.setStyleSheet("QTableWidget { font-size: 12px; }")
        layout.addWidget(QLabel("Cleaned Data:"))
        layout.addWidget(self.cleaned_data_preview)

        self.data_cleaning_tab.setLayout(layout)

    def init_table_creation_tab(self):
        layout = QVBoxLayout()

        # Table Name
        table_name_layout = QHBoxLayout()
        table_name_layout.addWidget(QLabel("Table Name:"))
        self.table_name_input = QLineEdit()
        table_name_layout.addWidget(self.table_name_input)
        layout.addLayout(table_name_layout)

        # Columns Table
        self.columns_table = QTableWidget()
        self.columns_table.setColumnCount(4)
        self.columns_table.setHorizontalHeaderLabels(["Column Name", "Data Type", "Primary Key", "Nullable"])
        self.columns_table.setRowCount(1)
        self.columns_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.columns_table.setAlternatingRowColors(True)

        # Set combo box delegate for Data Type column
        type_items = ["Integer", "String", "Float", "DateTime", "Date", "Boolean"]
        self.columns_table.setItemDelegateForColumn(1, ComboBoxDelegate(type_items, self.columns_table))

        # Set checkboxes for Primary Key and Nullable columns
        for row in range(self.columns_table.rowCount()):
            for col in [2, 3]:  # Primary Key and Nullable columns
                item = QTableWidgetItem()
                item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
                item.setCheckState(Qt.Checked if col == 3 else Qt.Unchecked)
                self.columns_table.setItem(row, col, item)

        layout.addWidget(QLabel("Table Columns:"))
        layout.addWidget(self.columns_table)

        # Add Column Button
        self.add_column_btn = QPushButton("Add Column")
        self.add_column_btn.clicked.connect(self.add_column_row)
        layout.addWidget(self.add_column_btn)

        # Constraints Group
        constraints_group = QGroupBox("Table Constraints")
        constraints_layout = QVBoxLayout()

        # Foreign Key Constraints Table
        self.constraints_table = QTableWidget()
        self.constraints_table.setColumnCount(4)
        self.constraints_table.setHorizontalHeaderLabels(["Type", "Source Column", "Target Table", "Target Column"])
        self.constraints_table.setRowCount(0)
        self.constraints_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.constraints_table.setAlternatingRowColors(True)

        # Add constraint button
        self.add_constraint_btn = QPushButton("Add Foreign Key Constraint")
        self.add_constraint_btn.clicked.connect(self.add_foreign_key_constraint)

        # Remove constraint button
        self.remove_constraint_btn = QPushButton("Remove Selected Constraint")
        self.remove_constraint_btn.clicked.connect(self.remove_constraint)

        constraints_btn_layout = QHBoxLayout()
        constraints_btn_layout.addWidget(self.add_constraint_btn)
        constraints_btn_layout.addWidget(self.remove_constraint_btn)

        constraints_layout.addWidget(self.constraints_table)
        constraints_layout.addLayout(constraints_btn_layout)
        constraints_group.setLayout(constraints_layout)
        layout.addWidget(constraints_group)

        # Create Table Button
        self.create_table_btn = QPushButton("Create Table")
        self.create_table_btn.clicked.connect(self.create_table)
        layout.addWidget(self.create_table_btn)

        # Existing Tables Group
        existing_tables_group = QGroupBox("Existing Tables")
        existing_tables_layout = QVBoxLayout()

        # Existing Tables List
        self.existing_tables_list = QListWidget()
        self.existing_tables_list.itemClicked.connect(self.show_table_columns)
        existing_tables_layout.addWidget(self.existing_tables_list)

        # Table Actions
        table_actions_layout = QHBoxLayout()

        self.truncate_table_btn = QPushButton("Truncate Table")
        self.truncate_table_btn.clicked.connect(self.truncate_selected_table)
        table_actions_layout.addWidget(self.truncate_table_btn)

        self.drop_table_btn = QPushButton("Drop Table")
        self.drop_table_btn.clicked.connect(self.drop_selected_table)
        self.drop_table_btn.setStyleSheet("background-color: #f44336;")
        table_actions_layout.addWidget(self.drop_table_btn)

        existing_tables_layout.addLayout(table_actions_layout)
        existing_tables_group.setLayout(existing_tables_layout)
        layout.addWidget(existing_tables_group)

        # Add Column to Existing Table
        self.add_to_existing_layout = QHBoxLayout()
        self.add_to_existing_layout.addWidget(QLabel("New Column:"))
        self.new_column_name = QLineEdit()
        self.add_to_existing_layout.addWidget(self.new_column_name)
        self.add_to_existing_layout.addWidget(QLabel("Type:"))
        self.new_column_type = QComboBox()
        self.new_column_type.addItems(["Integer", "String", "Float", "DateTime", "Date", "Boolean"])
        self.add_to_existing_layout.addWidget(self.new_column_type)
        self.add_column_to_table_btn = QPushButton("Add Column to Table")
        self.add_column_to_table_btn.clicked.connect(self.add_column_to_existing_table)
        self.add_to_existing_layout.addWidget(self.add_column_to_table_btn)
        layout.addLayout(self.add_to_existing_layout)

        self.table_creation_tab.setLayout(layout)

    def resizeEvent(self, event):
        """Переопределяем метод изменения размера для контроля максимальных размеров"""
        screen_geometry = QApplication.primaryScreen().availableGeometry()
        max_width = screen_geometry.width() * 0.9
        max_height = screen_geometry.height() * 0.9

        # Получаем новый размер из события
        new_size = event.size()

        # Корректируем размер, если он превышает максимальный
        if new_size.width() > max_width or new_size.height() > max_height:
            corrected_width = min(new_size.width(), max_width)
            corrected_height = min(new_size.height(), max_height)
            self.resize(int(corrected_width), int(corrected_height))
        else:
            super().resizeEvent(event)

    def init_field_mapping_tab(self):
        layout = QVBoxLayout()

        # Source and Target Selection
        source_target_layout = QHBoxLayout()

        # Source
        source_layout = QVBoxLayout()
        source_layout.addWidget(QLabel("Source:"))
        self.source_table_combo = QComboBox()
        source_layout.addWidget(self.source_table_combo)
        self.source_fields_list = QListWidget()
        source_layout.addWidget(self.source_fields_list)
        source_target_layout.addLayout(source_layout)

        # Target
        target_layout = QVBoxLayout()
        target_layout.addWidget(QLabel("Target:"))
        self.target_table_combo = QComboBox()
        self.target_table_combo.currentTextChanged.connect(self.update_target_fields)
        target_layout.addWidget(self.target_table_combo)
        self.target_fields_list = QListWidget()
        target_layout.addWidget(self.target_fields_list)
        source_target_layout.addLayout(target_layout)

        layout.addLayout(source_target_layout)

        # Mappings Table
        self.mappings_table = QTableWidget()
        self.mappings_table.setColumnCount(2)
        self.mappings_table.setHorizontalHeaderLabels(["Source Field", "Target Field"])
        self.mappings_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.mappings_table.setAlternatingRowColors(True)
        layout.addWidget(QLabel("Field Mappings:"))
        layout.addWidget(self.mappings_table)

        # Add Mapping Button
        self.add_mapping_btn = QPushButton("Add Mapping")
        self.add_mapping_btn.clicked.connect(self.add_field_mapping)
        layout.addWidget(self.add_mapping_btn)

        # SCD2 Options
        scd_layout = QHBoxLayout()
        self.scd2_checkbox = QCheckBox("Enable SCD2 (Slowly Changing Dimension Type 2)")
        scd_layout.addWidget(self.scd2_checkbox)
        self.scd2_start_date = QLineEdit("valid_from")
        scd_layout.addWidget(QLabel("Start Date Field:"))
        scd_layout.addWidget(self.scd2_start_date)
        self.scd2_end_date = QLineEdit("valid_to")
        scd_layout.addWidget(QLabel("End Date Field:"))
        scd_layout.addWidget(self.scd2_end_date)
        self.scd2_current_flag = QLineEdit("is_current")
        scd_layout.addWidget(QLabel("Current Flag Field:"))
        scd_layout.addWidget(self.scd2_current_flag)
        layout.addLayout(scd_layout)

        # Run ETL Button
        self.run_etl_btn = QPushButton("Run ETL Process")
        self.run_etl_btn.clicked.connect(self.run_etl_process)
        layout.addWidget(self.run_etl_btn)

        # Status
        self.etl_status = QLabel("")
        self.etl_status.setStyleSheet("font-weight: bold;")
        layout.addWidget(self.etl_status)

        self.field_mapping_tab.setLayout(layout)

    def init_query_tab(self):
        layout = QVBoxLayout()

        # SQL Query
        self.query_editor = QTextEdit()
        self.query_editor.setPlaceholderText("Enter your SQL query here...")
        self.query_editor.setStyleSheet("font-family: monospace; font-size: 12px;")
        layout.addWidget(self.query_editor)

        # Execute and Export Buttons
        query_buttons_layout = QHBoxLayout()

        self.execute_query_btn = QPushButton("Execute Query")
        self.execute_query_btn.clicked.connect(self.execute_query)
        query_buttons_layout.addWidget(self.execute_query_btn)

        self.export_results_btn = QPushButton("Export to CSV")
        self.export_results_btn.clicked.connect(self.export_query_results)
        self.export_results_btn.setEnabled(False)
        query_buttons_layout.addWidget(self.export_results_btn)

        layout.addLayout(query_buttons_layout)

        # Results
        self.query_results = QTableWidget()
        self.query_results.setAlternatingRowColors(True)
        self.query_results.setStyleSheet("QTableWidget { font-size: 12px; }")
        layout.addWidget(QLabel("Results:"))
        layout.addWidget(self.query_results)

        self.query_tab.setLayout(layout)

    def generate_sample_csv_files(self):
        # Create sample_data directory if it doesn't exist
        if not os.path.exists("sample_data"):
            os.makedirs("sample_data")

        # Generate customers.csv
        fake = Faker()
        customers = []
        for i in range(1, 101):
            customers.append({
                "customer_id": i,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "address": fake.street_address(),
                "city": fake.city(),
                "state": fake.state_abbr(),
                "zip_code": fake.zipcode(),
                "registration_date": fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
            })

        with open('sample_data/customers.csv', 'w', newline='') as csvfile:
            fieldnames = customers[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(customers)

        # Generate products.csv
        products = []
        categories = ["Electronics", "Clothing", "Home", "Books", "Toys"]
        for i in range(1, 101):
            products.append({
                "product_id": i,
                "name": fake.word().capitalize() + " " + fake.word().capitalize(),
                "category": random.choice(categories),
                "price": round(random.uniform(10, 500), 2),
                "in_stock": random.randint(0, 1000),
                "description": fake.sentence(),
                "created_at": fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
            })

        with open('sample_data/products.csv', 'w', newline='') as csvfile:
            fieldnames = products[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(products)

        # Generate orders.csv
        orders = []
        statuses = ["Pending", "Shipped", "Delivered", "Cancelled"]
        for i in range(1, 101):
            orders.append({
                "order_id": i,
                "customer_id": random.randint(1, 100),
                "order_date": fake.date_between(start_date='-1y', end_date='today').strftime('%Y-%m-%d'),
                "status": random.choice(statuses),
                "total_amount": round(random.uniform(50, 2000), 2),
                "shipping_address": fake.street_address(),
                "shipping_city": fake.city(),
                "shipping_state": fake.state_abbr(),
                "shipping_zip": fake.zipcode()
            })

        with open('sample_data/orders.csv', 'w', newline='') as csvfile:
            fieldnames = orders[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(orders)

        # Generate order_items.csv
        order_items = []
        for i in range(1, 501):
            order_items.append({
                "order_item_id": i,
                "order_id": random.randint(1, 100),
                "product_id": random.randint(1, 100),
                "quantity": random.randint(1, 10),
                "unit_price": round(random.uniform(10, 500), 2),
                "discount": round(random.uniform(0, 0.3), 2)
            })

        with open('sample_data/order_items.csv', 'w', newline='') as csvfile:
            fieldnames = order_items[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(order_items)

    def connect_to_db(self):
        db_type = self.db_type_combo.currentText()
        host = self.host_input.text()
        port = self.port_input.text()
        database = self.db_input.text()
        username = self.user_input.text()
        password = self.pwd_input.text()

        success, message = self.db_manager.connect(db_type, host, port, database, username, password)
        if success:
            self.connection_status.setText("Connected successfully")
            self.connection_status.setStyleSheet("color: #388e3c;")
            self.update_table_lists()
        else:
            self.connection_status.setText("Connection failed")
            self.connection_status.setStyleSheet("color: #d32f2f;")
            QMessageBox.critical(self, "Connection Error", message)

    def update_source_ui(self, source_type):
        if source_type == "CSV File":
            self.csv_source_ui.show()
            self.db_source_ui.hide()
        else:
            self.csv_source_ui.hide()
            self.db_source_ui.show()
            self.update_table_combo()

    def update_table_combo(self):
        self.table_combo.clear()
        tables = self.db_manager.get_tables()
        self.table_combo.addItems(tables)

    def update_table_lists(self):
        tables = self.db_manager.get_tables()
        self.existing_tables_list.clear()
        self.existing_tables_list.addItems(tables)

        self.source_table_combo.clear()
        self.source_table_combo.addItems(["CSV Data"] + tables)

        self.target_table_combo.clear()
        self.target_table_combo.addItems(tables)

        # Update constraints table target table combo
        for row in range(self.constraints_table.rowCount()):
            combo = self.constraints_table.cellWidget(row, 2)
            if combo:
                combo.clear()
                combo.addItems(tables)

    def update_target_fields(self, table_name):
        self.target_fields_list.clear()
        if table_name:
            columns = self.db_manager.get_table_columns(table_name)
            self.target_fields_list.addItems([col['name'] for col in columns])

    def load_csv_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File", "sample_data", "CSV Files (*.csv)")
        if file_path:
            try:
                self.source_data = pd.read_csv(file_path)
                self.csv_file_label.setText(os.path.basename(file_path))
                self.display_data_preview(self.source_data)
                self.update_source_fields()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to load CSV file: {str(e)}")

    def load_db_table(self):
        table_name = self.table_combo.currentText()
        if table_name:
            try:
                with self.db_manager.engine.connect() as conn:
                    self.source_data = pd.read_sql_table(table_name, conn)
                    self.display_data_preview(self.source_data)
                    self.update_source_fields()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to load table data: {str(e)}")

    def display_data_preview(self, data):
        self.data_preview.clear()
        if data is not None:
            self.data_preview.setRowCount(min(10, len(data)))
            self.data_preview.setColumnCount(len(data.columns))
            self.data_preview.setHorizontalHeaderLabels(data.columns)

            for i in range(min(10, len(data))):
                for j, col in enumerate(data.columns):
                    self.data_preview.setItem(i, j, QTableWidgetItem(str(data.iloc[i, j])))

    def update_source_fields(self):
        self.source_fields_list.clear()
        if self.source_data is not None:
            self.source_fields_list.addItems(self.source_data.columns.tolist())

    def remove_duplicates(self):
        if self.source_data is not None:
            initial_count = len(self.source_data)
            self.source_data.drop_duplicates(inplace=True)
            final_count = len(self.source_data)
            self.display_data_preview(self.source_data)
            self.cleaned_data_preview.clear()
            self.cleaned_data_preview.setRowCount(min(10, len(self.source_data)))
            self.cleaned_data_preview.setColumnCount(len(self.source_data.columns))
            self.cleaned_data_preview.setHorizontalHeaderLabels(self.source_data.columns)

            for i in range(min(10, len(self.source_data))):
                for j, col in enumerate(self.source_data.columns):
                    self.cleaned_data_preview.setItem(i, j, QTableWidgetItem(str(self.source_data.iloc[i, j])))

            QMessageBox.information(self, "Duplicates Removed",
                                    f"Removed {initial_count - final_count} duplicate rows. {final_count} rows remaining.")

    def add_column_row(self):
        row_count = self.columns_table.rowCount()
        self.columns_table.insertRow(row_count)

        # Add checkboxes for Primary Key and Nullable columns
        for col in [2, 3]:  # Primary Key and Nullable columns
            item = QTableWidgetItem()
            item.setFlags(Qt.ItemIsUserCheckable | Qt.ItemIsEnabled)
            item.setCheckState(Qt.Checked if col == 3 else Qt.Unchecked)
            self.columns_table.setItem(row_count, col, item)

    def add_foreign_key_constraint(self):
        row_count = self.constraints_table.rowCount()
        self.constraints_table.insertRow(row_count)

        # Constraint type
        constraint_type = QTableWidgetItem("foreign_key")
        self.constraints_table.setItem(row_count, 0, constraint_type)

        # Source column combo
        source_combo = QComboBox()
        if self.columns_table.rowCount() > 0:
            for row in range(self.columns_table.rowCount()):
                col_name = self.columns_table.item(row, 0)
                if col_name and col_name.text():
                    source_combo.addItem(col_name.text())
        self.constraints_table.setCellWidget(row_count, 1, source_combo)

        # Target table combo
        target_table_combo = QComboBox()
        tables = self.db_manager.get_tables()
        target_table_combo.addItems(tables)
        self.constraints_table.setCellWidget(row_count, 2, target_table_combo)

        # Target column combo (will be updated when target table changes)
        target_column_combo = QComboBox()
        self.constraints_table.setCellWidget(row_count, 3, target_column_combo)

        # Connect signal to update target columns when target table changes
        target_table_combo.currentTextChanged.connect(
            lambda table, row=row_count: self.update_target_column_combo(table, row))

        # Initial update of target columns
        if tables:
            self.update_target_column_combo(tables[0], row_count)

    def update_target_column_combo(self, table_name, row):
        target_column_combo = self.constraints_table.cellWidget(row, 3)
        if target_column_combo:
            target_column_combo.clear()
            columns = self.db_manager.get_table_columns(table_name)
            target_column_combo.addItems([col['name'] for col in columns])

    def remove_constraint(self):
        selected_rows = set(index.row() for index in self.constraints_table.selectedIndexes())
        for row in sorted(selected_rows, reverse=True):
            self.constraints_table.removeRow(row)

    def create_table(self):
        table_name = self.table_name_input.text()
        if not table_name:
            QMessageBox.warning(self, "Warning", "Please enter a table name")
            return

        # Собираем информацию о колонках из интерфейса
        columns = []
        for row in range(self.columns_table.rowCount()):
            col_name = self.columns_table.item(row, 0)
            col_type = self.columns_table.item(row, 1)
            pk_check = self.columns_table.item(row, 2)
            nullable_check = self.columns_table.item(row, 3)

            if col_name and col_type and col_name.text() and col_type.text():
                columns.append({
                    'name': col_name.text(),
                    'type': col_type.text(),
                    'primary_key': pk_check.checkState() == Qt.Checked,
                    'nullable': nullable_check.checkState() == Qt.Checked
                })

        if not columns:
            QMessageBox.warning(self, "Warning", "Please add at least one column")
            return

        # Собираем информацию об ограничениях из интерфейса
        constraints = []
        for row in range(self.constraints_table.rowCount()):
            constraint_type = self.constraints_table.item(row, 0)
            source_combo = self.constraints_table.cellWidget(row, 1)
            target_table_combo = self.constraints_table.cellWidget(row, 2)
            target_column_combo = self.constraints_table.cellWidget(row, 3)

            if (constraint_type and source_combo and
                    target_table_combo and target_column_combo):
                constraints.append({
                    'type': constraint_type.text(),
                    'source_column': source_combo.currentText(),
                    'target_table': target_table_combo.currentText(),
                    'target_column': target_column_combo.currentText()
                })

        # Вызываем создание таблицы
        success, message = self.db_manager.create_table(
            table_name=table_name,
            columns=columns,
            constraints=constraints
        )

        if success:
            QMessageBox.information(self, "Success", message)
            self.update_table_lists()
            self.table_name_input.clear()
            self.columns_table.setRowCount(1)
            self.columns_table.clearContents()
            self.constraints_table.setRowCount(0)
        else:
            QMessageBox.critical(self, "Error", message)

    def show_table_columns(self, item):
        table_name = item.text()
        columns = self.db_manager.get_table_columns(table_name)
        primary_keys = self.db_manager.get_primary_keys(table_name)
        foreign_keys = self.db_manager.get_foreign_keys(table_name)

        message = f"Table: {table_name}\n\nColumns:\n"
        for col in columns:
            message += f"- {col['name']} ({col['type']})"
            if col['name'] in primary_keys:
                message += " [Primary Key]"
            if not col['nullable']:
                message += " [NOT NULL]"
            message += "\n"

        if foreign_keys:
            message += "\nForeign Keys:\n"
            for fk in foreign_keys:
                message += f"- {fk['constrained_columns'][0]} → {fk['referred_table']}.{fk['referred_columns'][0]}\n"

        QMessageBox.information(self, f"Table Structure: {table_name}", message)

    def truncate_selected_table(self):
        selected_items = self.existing_tables_list.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a table to truncate")
            return

        table_name = selected_items[0].text()
        reply = QMessageBox.question(self, "Confirm Truncate",
                                     f"Are you sure you want to truncate table '{table_name}'? All data will be deleted!",
                                     QMessageBox.Yes | QMessageBox.No)

        if reply == QMessageBox.Yes:
            success, message = self.db_manager.truncate_table(table_name)
            if success:
                QMessageBox.information(self, "Success", message)
            else:
                QMessageBox.critical(self, "Error", message)

    def drop_selected_table(self):
        selected_items = self.existing_tables_list.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Warning", "Please select a table to drop")
            return

        table_name = selected_items[0].text()
        reply = QMessageBox.question(self, "Confirm Drop",
                                     f"Are you sure you want to drop table '{table_name}'? The table and all its data will be permanently deleted!",
                                     QMessageBox.Yes | QMessageBox.No)

        if reply == QMessageBox.Yes:
            success, message = self.db_manager.drop_table(table_name)
            if success:
                QMessageBox.information(self, "Success", message)
                self.update_table_lists()
            else:
                QMessageBox.critical(self, "Error", message)

    def add_column_to_existing_table(self):
        table_name = self.existing_tables_list.currentItem()
        if not table_name:
            QMessageBox.warning(self, "Warning", "Please select a table")
            return

        table_name = table_name.text()
        column_name = self.new_column_name.text()
        column_type = self.new_column_type.currentText()

        if not column_name:
            QMessageBox.warning(self, "Warning", "Please enter a column name")
            return

        # Check if column already exists
        columns = [col['name'] for col in self.db_manager.get_table_columns(table_name)]
        if column_name in columns:
            QMessageBox.warning(self, "Warning", f"Column '{column_name}' already exists in table '{table_name}'")
            return

        success, message = self.db_manager.add_column(table_name, column_name, column_type)
        if success:
            QMessageBox.information(self, "Success", message)
            self.new_column_name.clear()
            self.update_table_lists()
        else:
            QMessageBox.critical(self, "Error", message)

    def add_field_mapping(self):
        source_field = self.source_fields_list.currentItem()
        target_field = self.target_fields_list.currentItem()

        if not source_field or not target_field:
            QMessageBox.warning(self, "Warning", "Please select both source and target fields")
            return

        source_field = source_field.text()
        target_field = target_field.text()

        # Check if mapping already exists
        for row in range(self.mappings_table.rowCount()):
            existing_source = self.mappings_table.item(row, 0).text()
            if existing_source == source_field:
                QMessageBox.warning(self, "Warning", f"Source field '{source_field}' is already mapped")
                return

        row_count = self.mappings_table.rowCount()
        self.mappings_table.insertRow(row_count)
        self.mappings_table.setItem(row_count, 0, QTableWidgetItem(source_field))
        self.mappings_table.setItem(row_count, 1, QTableWidgetItem(target_field))

    def run_etl_process(self):
        if self.source_data is None:
            QMessageBox.warning(self, "Warning", "No source data loaded")
            return

        target_table = self.target_table_combo.currentText()
        if not target_table:
            QMessageBox.warning(self, "Warning", "Please select a target table")
            return

        # Create mapping dictionary
        mappings = {}
        for row in range(self.mappings_table.rowCount()):
            source = self.mappings_table.item(row, 0)
            target = self.mappings_table.item(row, 1)
            if source and target:
                mappings[source.text()] = target.text()

        if not mappings:
            QMessageBox.warning(self, "Warning", "No field mappings defined")
            return

        # Prepare data for insertion
        try:
            # Select only mapped columns and rename them
            etl_data = self.source_data[[src for src in mappings.keys()]].copy()
            etl_data.rename(columns=mappings, inplace=True)

            # Add SCD2 fields if enabled
            if self.scd2_checkbox.isChecked():
                current_time = datetime.now()
                etl_data[self.scd2_start_date.text()] = current_time
                etl_data[self.scd2_end_date.text()] = None
                etl_data[self.scd2_current_flag.text()] = True

            # Insert data
            success, message = self.db_manager.insert_data(target_table, etl_data)
            if success:
                self.etl_status.setText("ETL process completed successfully")
                self.etl_status.setStyleSheet("color: #388e3c;")
                QMessageBox.information(self, "Success", "Data loaded successfully")
            else:
                self.etl_status.setText(f"ETL process failed: {message}")
                self.etl_status.setStyleSheet("color: #d32f2f;")
                QMessageBox.critical(self, "Error", message)
        except Exception as e:
            self.etl_status.setText(f"ETL process failed: {str(e)}")
            self.etl_status.setStyleSheet("color: #d32f2f;")
            QMessageBox.critical(self, "Error", f"Failed to process data: {str(e)}")

    def execute_query(self):
        query = self.query_editor.toPlainText()
        if not query:
            QMessageBox.warning(self, "Warning", "Please enter a SQL query")
            return

        success, result = self.db_manager.execute_sql(query)
        if success:
            if isinstance(result, str):
                QMessageBox.information(self, "Success", result)
                self.query_results.clear()
                self.query_results.setRowCount(0)
                self.query_results.setColumnCount(0)
                self.export_results_btn.setEnabled(False)
            else:
                self.display_query_results(result)
                self.export_results_btn.setEnabled(True)
        else:
            QMessageBox.critical(self, "Error", result)
            self.export_results_btn.setEnabled(False)

    def display_query_results(self, results):
        self.query_results.clear()
        if results:
            self.query_results.setRowCount(len(results))
            self.query_results.setColumnCount(len(results[0]))

            # Set headers if available
            if hasattr(results[0], '_fields'):
                self.query_results.setHorizontalHeaderLabels(results[0]._fields)

            for i, row in enumerate(results):
                for j, value in enumerate(row):
                    self.query_results.setItem(i, j, QTableWidgetItem(str(value)))

    def export_query_results(self):
        if self.query_results.rowCount() == 0:
            QMessageBox.warning(self, "Warning", "No results to export")
            return

        file_path, _ = QFileDialog.getSaveFileName(self, "Save Results", "", "CSV Files (*.csv)")
        if file_path:
            try:
                with open(file_path, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)

                    # Write headers
                    headers = []
                    for col in range(self.query_results.columnCount()):
                        headers.append(self.query_results.horizontalHeaderItem(col).text())
                    writer.writerow(headers)

                    # Write data
                    for row in range(self.query_results.rowCount()):
                        row_data = []
                        for col in range(self.query_results.columnCount()):
                            item = self.query_results.item(row, col)
                            row_data.append(item.text() if item else "")
                        writer.writerow(row_data)

                    QMessageBox.information(self, "Success", f"Results exported to {file_path}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to export results: {str(e)}")


def main():
    app = QApplication(sys.argv)

    # Set application font
    font = QFont()
    font.setFamily("Arial")
    font.setPointSize(10)
    app.setFont(font)

    window = ETLApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()