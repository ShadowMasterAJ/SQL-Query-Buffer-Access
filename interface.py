from PyQt6.QtWidgets import QHBoxLayout, QMainWindow, QLabel, QTextEdit, QPushButton, QVBoxLayout, QWidget, QSizePolicy, QScrollArea, QApplication

from explore import *

# TODO: add functionality for another window to retrieve user db details (can follow ref github for this)


class SQLQueryExecutor(QMainWindow):
    def __init__(self):
        super().__init__()
        self._setup_main_window()
        self._setup_central_widget()  # Call this to set up the central widget
        self._setup_query_components()
        self._setup_disk_block_components()

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

        self.submit_button = QPushButton("Execute Query")
        self.submit_button.clicked.connect(self.on_submit_query)
        self.queryInputLayout.addWidget(self.submit_button)

        self.quit_button = QPushButton("Quit", self)
        self.quit_button.clicked.connect(lambda: QApplication.quit())
        self.quit_button.setSizePolicy(
            QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed,)
        self.queryInputLayout.addWidget(self.quit_button)

        self.main_layout.addLayout(self.queryInputLayout, stretch=2)

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

    def on_submit_query(self):
        query = self.query_text.toPlainText()
        conn = connect_to_db()
        qep = execute_query(conn, query)
        getAllRelationsInfo(qep)
        self.show_disk_block_info()
        visualize_qep(qep)

    def show_disk_block_info(self):
        query = self.query_text.toPlainText()
        conn = connect_to_db()
        qep = execute_query(conn, query)
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
                    lambda: self.show_block_content(conn, relation, block_id))
                relation_blocks_layout.addWidget(button)

            relations_layout.addWidget(relation_blocks_scroll_area)
            relations_column_widget = QWidget()
            relations_column_widget.setLayout(relations_layout)
            self.relation_columns.addWidget(relations_column_widget)

    ''' TODO:
    - fix 1 below function to update properly when clicking some other block
    
    - fix 2
        1. update `get_block_contents` to handle ranged ids (those shown in brackets eg (322,3324) is blocks from id 322 to 3324)
        2. currently for single block ids shown as vertically ([relation block] id heading label followed by the content text objects)
        3. for ranged queries show sequentially ([relation name] block id [start ...  end] heading label followed by the content text objects for each id)
    '''

    def show_block_content(self, conn, relation, block_id):
        content = get_block_contents(conn, relation, block_id)

        # # Clear existing widgets from the layout
        for i in reversed(range(self.block_contents_layout.count())):
            widget_to_remove = self.block_contents_layout.itemAt(i).widget()
            if widget_to_remove is not None:
                self.block_contents_layout.removeWidget(widget_to_remove)
                widget_to_remove.deleteLater()

        self.block_contents_layout.addWidget(
            QLabel(f"Relation: {relation} | Block ID: {block_id}"))

        for item in content:
            block_content_text = QTextEdit()
            block_content_text.setReadOnly(True)
            block_content_text.setPlainText(str(item).replace("  ", " "))
            self.block_contents_layout.addWidget(block_content_text)
