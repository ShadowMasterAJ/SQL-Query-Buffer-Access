import json
import re
import psycopg2
import matplotlib.pyplot as plt
import networkx as nx


def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="root",
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
        qep = make_json_parsable(str(qep[0][0]))
        return qep

    except Exception as e:
        print(f"Error executing query: {e}")
        return None


def get_disk_blocks_accessed(conn, query):
    """
    Retrieves disk block access information for a given SQL query.

    Parameters:
    conn (psycopg2.connection): A connection to the PostgreSQL database.
    query (str): The SQL query for which disk block access information is to be retrieved.

    Returns:
    dict: A dictionary containing disk block access information.
    """
    # Execute the provided SQL query
    with conn.cursor() as cursor:
        cursor.execute(query)
        # Optionally, fetch and use query results if needed
        # query_results = cursor.fetchall()

    # Query the statistics view to get block access info
    stats_query = """
    SELECT 
        relname, 
        heap_blks_read,  -- Blocks read from disk
        heap_blks_hit    -- Blocks found in cache
    FROM 
        pg_statio_user_tables;
    """

    with conn.cursor() as cursor:
        cursor.execute(stats_query)
        stats_results = cursor.fetchall()

    # Format the results into a dictionary
    block_access_info = {
        "table": [],
        "blocks_read": [],
        "blocks_hit": []
    }
    for row in stats_results:
        block_access_info["table"].append(row[0])
        block_access_info["blocks_read"].append(row[1])
        block_access_info["blocks_hit"].append(row[2])

    for k,v in block_access_info.items():
        print(k,':',v)
    return block_access_info


def parse_and_visualize_qep(qep_json_str):
    # Parse the JSON string
    try:
        qep_json = json.loads(qep_json_str)
        print('JSON parsed!')
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
                graph.add_edge(current_label,parent_label)

            # Process child nodes if any
            if 'Plans' in node and isinstance(node['Plans'], list):
                for child in node['Plans']:
                    process_node(child, current_label)

    # Start processing from the root node
    for entry in qep_json:
        process_node(entry['Plan'])

    # Visualization
    pos = nx.spring_layout(graph)
    nx.draw(graph, pos, with_labels=True, node_color='skyblue', edge_color='black',
            node_size=2000, arrowstyle='->', arrowsize=20)
    plt.title('Query Execution Plan')
    plt.show()
