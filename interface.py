from PyQt6.QtWidgets import QHBoxLayout, QMainWindow, QLabel, QTextEdit, QPushButton, QVBoxLayout, QWidget, QSizePolicy, QScrollArea, QApplication, QDialog, QLabel, QLineEdit, QVBoxLayout, QPushButton
from explore import *
from PyQt6.QtCore import QTimer
import sys
import socket
import time
import subprocess
import webbrowser
import threading
import logging

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

class UserDetailsDialog(QDialog):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("User Details")
        self.setGeometry(400, 200, 400, 200)

        self.host_label = QLabel("Host:")
        self.host_line_edit = QLineEdit()
        self.db_label = QLabel("Database:")
        self.db_line_edit = QLineEdit()
        self.username_label = QLabel("Username:")
        self.username_line_edit = QLineEdit()
        self.password_label = QLabel("Password:")
        self.password_line_edit = QLineEdit()
        self.password_line_edit.setEchoMode(QLineEdit.EchoMode.Password)

        self.connected_user_label = QLabel("Connected User: N/A") 

        self.submit_button = QPushButton("Connect")
        self.submit_button.clicked.connect(self.accept)

        layout = QVBoxLayout()
        layout.addWidget(self.host_label)
        layout.addWidget(self.host_line_edit)
        layout.addWidget(self.db_label)
        layout.addWidget(self.db_line_edit)
        layout.addWidget(self.username_label)
        layout.addWidget(self.username_line_edit)
        layout.addWidget(self.password_label)
        layout.addWidget(self.password_line_edit)
        layout.addWidget(self.connected_user_label)
        layout.addWidget(self.submit_button)

        self.setLayout(layout)

    def get_user_details(self):
        host = self.host_line_edit.text()
        db = self.db_line_edit.text()
        username = self.username_line_edit.text()
        password = self.password_line_edit.text()
        return host, db, username, password
    def set_connected_user_label(self, username):
        self.connected_user_label.setText(f"Connected User: {username}")


class SQLQueryExecutor(QMainWindow):
    def __init__(self):
        super().__init__()
        self._setup_main_window()
        self._setup_central_widget()  # Call this to set up the central widget
        self._setup_query_components()
        self._setup_disk_block_components()
    
        self.user_details_dialog = UserDetailsDialog()

        self.user_details_dialog = UserDetailsDialog()

    def _setup_main_window(self):
        """Set up the main window properties."""
        self.setWindowTitle("SQL Query Executor")
        self.setMinimumSize(1700, 1000)

    def _setup_central_widget(self):
        """Set up the central widget and main layout."""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.main_layout = QHBoxLayout(central_widget)

    def _setup_query_components(self):
        """Set up the components for SQL query input."""
        self.queryInputLayout = QVBoxLayout()

        query_label = QLabel("Enter SQL Query:")
        self.queryInputLayout.addWidget(query_label)

        self.query_text = QTextEdit()
        self.queryInputLayout.addWidget(self.query_text)

        self.user_details_button = QPushButton("User Details")
        self.user_details_button.clicked.connect(self.show_user_details_dialog)
        self.queryInputLayout.addWidget(self.user_details_button)

        self.submit_button = QPushButton("Execute Query")
        self.submit_button.clicked.connect(self.on_submit_query)
        self.queryInputLayout.addWidget(self.submit_button)

        self.visualise_query = QPushButton("Visualise QEP")
        self.visualise_query.clicked.connect(self.on_click)
        self.queryInputLayout.addWidget(self.visualise_query)
        
        self.quit_button = QPushButton("Quit", self)
        self.quit_button.clicked.connect(lambda: QApplication.quit())
        self.quit_button.setSizePolicy(
            QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed,)
        self.queryInputLayout.addWidget(self.quit_button)

        self.main_layout.addLayout(self.queryInputLayout, stretch=2)

    def show_user_details_dialog(self):
        result = self.user_details_dialog.exec()
        if result == QDialog.DialogCode.Accepted:
            host, db, username, password = self.user_details_dialog.get_user_details()
            # Use the obtained user details as needed (e.g., connect to the database)

            conn = connect_to_db(host, db, username, password)

            if conn:
                # Update the connected user label in the UserDetailsDialog
                self.user_details_dialog.set_connected_user_label(username)

    def _setup_disk_block_components(self):
        """Set up components for displaying disk block information."""
        self.disk_block_layout = QVBoxLayout()
        self.disk_block_label = QLabel("Disk Block Information:")
        self.disk_block_layout.addWidget(self.disk_block_label)

        self.relation_columns = QHBoxLayout()
        self.relation_blocks_scroll_area = QScrollArea()
        self.relation_blocks_scroll_area.setWidgetResizable(True)
        self.relation_blocks_container = QWidget()
        self.relation_blocks_container.setLayout(self.relation_columns)
        self.relation_blocks_scroll_area.setWidget(
            self.relation_blocks_container)

        self.disk_block_layout.addWidget(self.relation_blocks_scroll_area)

        # Setup for block contents area
        self.block_contents = QScrollArea()
        self.block_contents.setWidgetResizable(True)
        self.block_contents_container = QWidget()
        self.block_contents_layout = QVBoxLayout()
        self.block_contents_container.setLayout(self.block_contents_layout)
        self.block_contents.setWidget(self.block_contents_container)

        # Add the scroll area for block contents to the disk block layout
        self.disk_block_layout.addWidget(self.block_contents)

        self.main_layout.addLayout(self.disk_block_layout, stretch=3)

    def on_click(self):
        logging.info("Button clicked, launching visualise_qep thread...")
        threading.Thread(target=self.visualise_qep, daemon=True).start()

    def visualise_qep(self):
        try:
            subprocess.Popen(["npm", "run", "serve"], cwd="pev2_component")
            logging.info("Server starting...")
            self.wait_for_server_ready()
        except Exception as e:
            logging.error(f"Error starting server: {e}")

    def wait_for_server_ready(self):
        server_ready = False
        for _ in range(10):  # Try for 30 seconds
            try:
                with socket.create_connection(("localhost", 8080), timeout=1):
                    server_ready = True
                    break
            except OSError:
                time.sleep(1)

        if server_ready:
            logging.info("Server ready, opening browser...")
            webbrowser.open("http://localhost:8080")
        else:
            logging.warning("Server not ready after 30 seconds.")
        
    def on_submit_query(self):
        query = self.query_text.toPlainText()
        host, db, username, password = self.user_details_dialog.get_user_details()
        conn = connect_to_db(host, db, username, password)
        print('DB Details',host, db, username, password)
        qep = execute_query(conn, query)
        getAllRelationsInfo(qep)
        self.show_disk_block_info(conn,qep)
        # visualize_qep(qep)

    def show_disk_block_info(self,conn,qep):
        disk_blocks_info = get_disk_blocks_accessed(conn, qep)

        # Clear existing columns
        for i in reversed(range(self.relation_columns.count())):
            widget_to_remove = self.relation_columns.itemAt(i).widget()
            if widget_to_remove is not None:
                widget_to_remove.deleteLater()

        # Create columns for each relation
        for relation, block_ids in disk_blocks_info.items():
            relations_layout = QVBoxLayout()

            relation_label = QLabel(f"Relation: {relation}")
            relations_layout.addWidget(relation_label)

            relation_blocks_scroll_area = QScrollArea()
            relation_blocks_scroll_area.setWidgetResizable(True)
            relation_blocks_container = QWidget()
            relation_blocks_layout = QVBoxLayout(relation_blocks_container)
            relation_blocks_scroll_area.setWidget(relation_blocks_container)
            # Create buttons for each block ID
            for block_id in block_ids:
                button = QPushButton(f"Block ID: {block_id}")
                button.setSizePolicy(
                    QSizePolicy.Policy.MinimumExpanding, QSizePolicy.Policy.MinimumExpanding)
                # Connect button to function
                button.clicked.connect(
                    lambda _, r=relation,b=block_id: self.show_block_content(conn, r, b))
                relation_blocks_layout.addWidget(button)

            relations_layout.addWidget(relation_blocks_scroll_area)
            relations_column_widget = QWidget()
            relations_column_widget.setLayout(relations_layout)
            self.relation_columns.addWidget(relations_column_widget)

    def show_block_content(self, conn, relation, block_id):
        content = get_block_contents(conn, relation, block_id)
        bufferValue= get_No_Of_Buffers(conn,relation,block_id)
        if 'shared hit'
        # # Clear existing widgets from the layout
        for i in reversed(range(self.block_contents_layout.count())):
            widget_to_remove = self.block_contents_layout.itemAt(i).widget()
            if widget_to_remove is not None:
                self.block_contents_layout.removeWidget(widget_to_remove)
                widget_to_remove.deleteLater()

        self.block_contents_layout.addWidget(
            QLabel(f"Relation: {relation} | Block ID: {block_id} | {bufferValue}"))

        for item in content:
            block_content_text = QTextEdit()
            block_content_text.setReadOnly(True)
            block_content_text.setPlainText(str(item).replace("  ", " "))
            self.block_contents_layout.addWidget(block_content_text)
