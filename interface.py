from PyQt6.QtWidgets import QApplication, QMainWindow, QLabel, QTextEdit, QPushButton, QVBoxLayout, QWidget

from explore import connect_to_db, execute_query, get_disk_blocks_accessed, parse_and_visualize_qep

class SQLQueryExecutor(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("SQL Query Executor")
        self.setMinimumSize(400, 200)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout()

        query_label = QLabel("Enter SQL Query:")
        layout.addWidget(query_label)

        self.query_text = QTextEdit()
        layout.addWidget(self.query_text)

        submit_button = QPushButton("Submit")
        submit_button.clicked.connect(self.on_submit_query)
        layout.addWidget(submit_button)

        # Widgets for displaying disk block information
        self.disk_block_label = QLabel("Disk Block Information:")
        layout.addWidget(self.disk_block_label)

        self.disk_block_text = QTextEdit()
        self.disk_block_text.setReadOnly(True)
        layout.addWidget(self.disk_block_text)

        # Button to display disk block information
        show_blocks_button = QPushButton("Show Disk Blocks Info")
        show_blocks_button.clicked.connect(self.show_disk_block_info)
        layout.addWidget(show_blocks_button)

        central_widget.setLayout(layout)

    def on_submit_query(self):
        query = self.query_text.toPlainText()
        conn = connect_to_db()
        qep = execute_query(conn, query)
        parse_and_visualize_qep(qep)

    def show_disk_block_info(self):
        # Get the current query from the QTextEdit widget
        query = self.query_text.toPlainText()
        conn = connect_to_db()
        disk_blocks_info = get_disk_blocks_accessed(conn, query)
        
