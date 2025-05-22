import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
                             QLineEdit, QPushButton, QComboBox, QTableWidget, QTableWidgetItem,
                             QTabWidget, QFileDialog, QMessageBox, QTextEdit, QListWidget, QCheckBox)
from PyQt5.QtCore import Qt
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, inspect, text
from sqlalchemy.types import Integer, String, Float, DateTime
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime


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

    def create_table(self, table_name, columns):
        columns = [Column(col['name'], self._get_sqlalchemy_type(col['type'])) for col in columns]
        table = Table(table_name, self.metadata, *columns)
        try:
            table.create(self.engine)
            return True, f"Table {table_name} created successfully"
        except SQLAlchemyError as e:
            return False, str(e)

    def add_column(self, table_name, column_name, column_type):
        try:
            with self.engine.connect() as connection:
                sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
                connection.execute(text(sql))
                connection.commit()
                return True, f"Column {column_name} added to {table_name}"
        except SQLAlchemyError as e:
            return False, str(e)

    def insert_data(self, table_name, data):
        try:
            with self.engine.connect() as connection:
                data.to_sql(table_name, connection, if_exists='append', index=False)
                return True, f"Data inserted into {table_name}"
        except SQLAlchemyError as e:
            return False, str(e)

    def _get_sqlalchemy_type(self, type_str):
        type_mapping = {
            'Integer': Integer,
            'String': String,
            'Float': Float,
            'DateTime': DateTime
        }
        return type_mapping.get(type_str, String)


class ETLApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ETL Flow Manager")
        self.setGeometry(100, 100, 1000, 800)

        self.db_manager = DatabaseManager()
        self.source_data = None
        self.current_etl_flow = {}

        self.init_ui()

    def init_ui(self):
        self.tabs = QTabWidget()

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
        self.columns_table.setColumnCount(2)
        self.columns_table.setHorizontalHeaderLabels(["Column Name", "Data Type"])
        self.columns_table.setRowCount(1)
        layout.addWidget(QLabel("Table Columns:"))
        layout.addWidget(self.columns_table)

        # Add Column Button
        self.add_column_btn = QPushButton("Add Column")
        self.add_column_btn.clicked.connect(self.add_column_row)
        layout.addWidget(self.add_column_btn)

        # Create Table Button
        self.create_table_btn = QPushButton("Create Table")
        self.create_table_btn.clicked.connect(self.create_table)
        layout.addWidget(self.create_table_btn)

        # Existing Tables
        layout.addWidget(QLabel("Existing Tables:"))
        self.existing_tables_list = QListWidget()
        self.existing_tables_list.itemClicked.connect(self.show_table_columns)
        layout.addWidget(self.existing_tables_list)

        # Add Column to Existing Table
        self.add_to_existing_layout = QHBoxLayout()
        self.add_to_existing_layout.addWidget(QLabel("New Column:"))
        self.new_column_name = QLineEdit()
        self.add_to_existing_layout.addWidget(self.new_column_name)
        self.add_to_existing_layout.addWidget(QLabel("Type:"))
        self.new_column_type = QComboBox()
        self.new_column_type.addItems(["Integer", "String", "Float", "DateTime"])
        self.add_to_existing_layout.addWidget(self.new_column_type)
        self.add_column_to_table_btn = QPushButton("Add Column to Table")
        self.add_column_to_table_btn.clicked.connect(self.add_column_to_existing_table)
        self.add_to_existing_layout.addWidget(self.add_column_to_table_btn)
        layout.addLayout(self.add_to_existing_layout)

        self.table_creation_tab.setLayout(layout)

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
        layout.addWidget(self.etl_status)

        self.field_mapping_tab.setLayout(layout)

    def init_query_tab(self):
        layout = QVBoxLayout()

        # SQL Query
        self.query_editor = QTextEdit()
        self.query_editor.setPlaceholderText("Enter your SQL query here...")
        layout.addWidget(self.query_editor)

        # Execute Button
        self.execute_query_btn = QPushButton("Execute Query")
        self.execute_query_btn.clicked.connect(self.execute_query)
        layout.addWidget(self.execute_query_btn)

        # Results
        self.query_results = QTableWidget()
        layout.addWidget(QLabel("Results:"))
        layout.addWidget(self.query_results)

        self.query_tab.setLayout(layout)

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
            self.update_table_lists()
        else:
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

    def update_target_fields(self, table_name):
        self.target_fields_list.clear()
        if table_name:
            columns = self.db_manager.get_table_columns(table_name)
            self.target_fields_list.addItems([col['name'] for col in columns])

    def load_csv_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV File", "", "CSV Files (*.csv)")
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

    def create_table(self):
        table_name = self.table_name_input.text()
        if not table_name:
            QMessageBox.warning(self, "Warning", "Please enter a table name")
            return

        columns = []
        for row in range(self.columns_table.rowCount()):
            col_name = self.columns_table.item(row, 0)
            col_type = self.columns_table.item(row, 1)

            if col_name and col_type and col_name.text() and col_type.text():
                columns.append({
                    'name': col_name.text(),
                    'type': col_type.text()
                })

        if not columns:
            QMessageBox.warning(self, "Warning", "Please add at least one column")
            return

        success, message = self.db_manager.create_table(table_name, columns)
        if success:
            QMessageBox.information(self, "Success", message)
            self.update_table_lists()
            self.table_name_input.clear()
            self.columns_table.setRowCount(1)
            self.columns_table.clearContents()
        else:
            QMessageBox.critical(self, "Error", message)

    def show_table_columns(self, item):
        table_name = item.text()
        columns = self.db_manager.get_table_columns(table_name)
        QMessageBox.information(self, f"Columns in {table_name}",
                                "\n".join([f"{col['name']} ({col['type']})" for col in columns]))

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
                QMessageBox.information(self, "Success", "Data loaded successfully")
            else:
                self.etl_status.setText(f"ETL process failed: {message}")
                QMessageBox.critical(self, "Error", message)
        except Exception as e:
            self.etl_status.setText(f"ETL process failed: {str(e)}")
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
            else:
                self.display_query_results(result)
        else:
            QMessageBox.critical(self, "Error", result)

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


def main():
    app = QApplication(sys.argv)
    window = ETLApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()