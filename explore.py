import ast
import json
import re
import psycopg2
import matplotlib.pyplot as plt
import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout


class Node:
    """
    Represents a node in a query execution plan.
    """

    def __init__(self, qep_json):
        """
        Initializes the Node with data from a JSON object.

        :param plan_data: A dictionary representing the plan data for this node.
        """
        self.node_type = qep_json.get("Node Type")
        self.plan = {key: value for key, value in qep_json.items(
        ) if key == "Relation Name" or key == "Hash Cond" or key == "Parent Relationship"}
        self.children = [Node(child) for child in qep_json.get("Plans", [])]

    def __str__(self):
        """
        Returns a string representation of the node.
        """
        child_str = ', '.join([str(child) for child in self.children])
        return f"Node Type: {self.node_type}, Plan: {self.plan}, Children: [{child_str}]"

    def print_tree(self, level=0):
        """
        Prints the node and its children in a tree-like structure.

        :param level: The current depth level in the tree (used for indentation).
        """
        indent = "\t" * level  # Two spaces per level of depth
        node_info = f"{indent}Node:\t{self.node_type}\n{indent}Plan:\t{self.plan}"
        print(node_info)
        for child in self.children:
            child.print_tree(level + 1)


def connect_to_db(host='localhost', db='postgres', username='postgres', password='root'):
    try:
        if host==None or db==None or username==None or password==None:
            host='localhost'
            db='postgres'
            username='postgres'
            password='root'
        conn = psycopg2.connect(
            host=host,
            database=db,
            user=username,
            password=password,
            port=5432
        )
        print("CONNECTED TO DATABASE!")
        return conn
    except Exception as e:
        print(f"Error: {e}")
        return None

def make_json_parsable(data):
    # Replace 'True' and 'False' with lowercase equivalents
    data = data.replace('True', 'true').replace('False', 'false')

    # Replace all single quotes with double quotes
    data = data.replace("'", "\"")

    # Regular expression to identify the value for the "Filter" key
    def fix_filter_quotes(match):
        value = match.group(0)
        num_quotes = value.count('"')
        # print('matched:', value)

        if num_quotes > 2:
            # Replace all double quotes with single quotes
            main_content = value[1:-1].replace('"', "'")
            fixed_value = f'"{main_content}"'
            # print('fixed:', fixed_value)
            return fixed_value
        else:
            return value

    # Apply the regular expression to the value of the "Filter" key
    parsable_data = re.sub(
        r'(?<="Filter":\s)([^"]|"(?!,))*"', fix_filter_quotes, data)

    return parsable_data

def execute_query(conn, query):
    cursor = conn.cursor()
    try:
        cursor.execute(f"EXPLAIN \
                        ( \
                            BUFFERS TRUE, \
                            COSTS TRUE, \
                            SETTINGS TRUE, \
                            WAL TRUE, \
                            TIMING TRUE, \
                            SUMMARY TRUE, \
                            ANALYZE TRUE, \
                            FORMAT JSON \
                        ) {query}")
        qep = cursor.fetchall()
        # print(qep)
        qep = make_json_parsable(str(qep[0][0]))
        return qep

    except psycopg2.Error as e:
        error_message = f"Error executing query: {e}"
        print(error_message)
        return None

    except Exception as e:
        print(f"Error executing query: {e}")
        return None

def get_No_Of_Buffers(conn,relation,block_id):
    cursor = conn.cursor()
    try:
        query = f"""
        EXPLAIN (ANALYZE , BUFFERS TRUE, COSTS FALSE)
        SELECT ctid, *
        FROM {relation}
        WHERE (ctid::text::point)[0]::bigint = {block_id};
        """
        cursor.execute(query)
        
        qep=cursor.fetchall()
        for tup in qep:
            for value in tup:
                if ("Buffers:" in value):
                    return value
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    
def get_block_contents(conn, relation, block_id):
    """Get contents of block with block_id of relation.

    Keyword arguments:
    block_id -- block_id of block
    relation -- relation that contains block
    Return: list of tuples with block_id of relation
    """

    cursor = conn.cursor()
    if isinstance(block_id, tuple):
        query = f"""
        SELECT ctid, *
        FROM {relation}
        WHERE (ctid::text::point)[0]::bigint BETWEEN {block_id[0]} and {block_id[1]};
        """
    else:
        query = f"""
        SELECT ctid, *
        FROM {relation}
        WHERE (ctid::text::point)[0]::bigint = {block_id};
        """
    try:
        cursor.execute(query)
        content = cursor.fetchall()
        if content:
            return content
        else:
            raise ValueError("No data found for the given block ID.")
    except psycopg2.Error as e:
        error_message = f"Error executing query: {e}"
        print(error_message)
        return None
    except Exception as e:
        error_message = f"Unexpected error: {e}"
        print(error_message)
        return None
    
def get_relation_block_ids(conn, relation_name):
    """Get distinct block IDs for a given relation.

    Keyword arguments:
    relation_name -- name of the relation
    Return: list of block IDs
    """
    cursor = conn.cursor()
    query = f"""
    SELECT DISTINCT (ctid::text::point)[0]::bigint AS block_id
    FROM {relation_name};
    """
    cursor.execute(query)
    out = cursor.fetchall()
    if out is not None:
        ls = [row[0] for row in out]
        # print(relation_name, 'blocks:', ls)
        return ls
    else:
        raise ValueError("No blocks found for the given relation.")

def get_disk_blocks_accessed(conn, qep_json_str):
    blocks_accessed = {}
    qep = getAllRelationsInfo(qep_json_str=qep_json_str)

    def process_node(node):
        match node.node_type:
            case "Seq Scan" | "Parallel Seq Scan":
                relation_name = node.plan["Relation Name"]
                blocks_accessed[relation_name] = {
                    block_id
                    for block_id in get_relation_block_ids(conn, relation_name)
                }

            case "Index Scan":
                relation_name = node.plan["Relation Name"]
                index_cond = node.plan.get(
                    "Index Cond") or node.plan.get("Filter")
                if index_cond:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            f"SELECT ctid, * FROM {relation_name} WHERE {index_cond};"
                        )
                        records = cursor.fetchall()
                        block_ids = set()
                        for record in records:
                            block_id, _ = ast.literal_eval(record[0])
                            block_ids.add(block_id)
                        blocks_accessed[relation_name] = block_ids

            case _:
                # Generic handler for other node types
                for child in node.children:
                    child_blocks_accessed = process_node(child)
                    for relation, block_ids in child_blocks_accessed.items():
                        if relation in blocks_accessed:
                            blocks_accessed[relation].update(block_ids)
                        else:
                            blocks_accessed[relation] = block_ids

        return blocks_accessed

    # Starting the recursion with the root node
    return process_node(qep[0])

def visualize_qep(qep_json_str):
    # Parse the JSON string
    try:
        qep_json = json.loads(qep_json_str)
        print('JSON parsed!')
        with open('plan.json', 'w') as outfile:
            json.dump(qep_json, outfile)

    except json.JSONDecodeError as e:
        print('Error in JSON:', e)
        return

    # Create a directed graph
    graph = nx.DiGraph()

    # Recursive function to process each node in the QEP
    def process_node(node, parent_label=None):
        if 'Node Type' in node:
            # Construct label for the current node
            current_label = node['Node Type']
            if 'Relation Name' in node:
                current_label += f" on {node['Relation Name']}"
            if 'Alias' in node:
                current_label += f" (alias: {node['Alias']})"

            # Add node to the graph
            graph.add_node(current_label)

            # Connect to parent node if exists
            if parent_label:
                graph.add_edge(current_label, parent_label)

            # Process child nodes if any
            if 'Plans' in node and isinstance(node['Plans'], list):
                for child in node['Plans']:
                    process_node(child, current_label)

    # Start processing from the root node
    for entry in qep_json:
        process_node(entry['Plan'])

    # TODO: see if can be made better than what it is right now (replicate pgadmin graph though optional)
    # Visualization
    pos = graphviz_layout(graph)
    nx.draw(graph, pos, with_labels=True, node_color='skyblue', edge_color='black',
            node_size=2000, arrowstyle='->', arrowsize=20)
    plt.title('Query Execution Plan')
    plt.show()

def getAllRelationsInfo(qep_json_str):
    qep_json = json.loads(qep_json_str)
    nodes = [Node(plan["Plan"]) for plan in qep_json]
    for node in nodes:
        node.print_tree()
    return nodes
