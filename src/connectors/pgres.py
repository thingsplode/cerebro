import psycopg2
from psycopg2 import sql
import os
import json
import logging

# Set up logging
logger = logging.getLogger(__name__)

def get_db_connection_params(database=None):
    """
    Retrieve database connection parameters from environment variables.
    """
    return {
        'dbname': database or os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432')
    }

def create_connection():
    """
    Create a new database connection.
    """
    conn_params = get_db_connection_params()
    return psycopg2.connect(**conn_params)

def extract_databases(filter_builtin_databases=True, print_results=False) -> list[str]:
    """
    Extract non-system databases from PostgreSQL.
    """
    conn = create_connection()
    try:
        with conn.cursor() as cursor:
            filter = " AND datname NOT IN ('postgres', 'template0', 'template1')" if filter_builtin_databases else ""
            cursor.execute(f"""
                SELECT datname FROM pg_database
                WHERE datistemplate = false{filter}
            """)
            databases = [row[0] for row in cursor.fetchall()]
            if print_results:
                print(f"Databases: {databases}")
            return databases
    finally:
        conn.close()

def extract_schema(database, print_results=False) -> dict:
    """
    Extract schema information for the specified database.
    Returns a dictionary containing the schema information for all tables.
    """
    conn_params = get_db_connection_params(database=database)
    conn = psycopg2.connect(**conn_params)
    schema_info = {}
    try:
        with conn.cursor() as cursor:
            schema_query = sql.SQL("""
                SELECT 
                    c.table_name, 
                    c.column_name, 
                    c.data_type,
                    c.character_maximum_length,
                    c.is_nullable,
                    c.column_default,
                    pg_catalog.col_description(format('%s.%s', c.table_schema, c.table_name)::regclass::oid, c.ordinal_position) as column_description,
                    CASE 
                        WHEN pk.constraint_type IS NOT NULL THEN 'PRIMARY KEY'
                        WHEN fk.constraint_name IS NOT NULL THEN 'FOREIGN KEY'
                        ELSE NULL
                    END as constraint_type,
                    fk.foreign_table_name,
                    fk.foreign_column_name
                FROM 
                    information_schema.columns c
                LEFT JOIN (
                    SELECT ku.table_name, ku.column_name, tc.constraint_type
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage ku ON tc.constraint_name = ku.constraint_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                ) pk ON c.table_name = pk.table_name AND c.column_name = pk.column_name
                LEFT JOIN (
                    SELECT 
                        kcu.table_name, 
                        kcu.column_name, 
                        kcu.constraint_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM 
                        information_schema.key_column_usage kcu
                    JOIN information_schema.referential_constraints rc ON kcu.constraint_name = rc.constraint_name
                    JOIN information_schema.constraint_column_usage ccu ON rc.unique_constraint_name = ccu.constraint_name
                ) fk ON c.table_name = fk.table_name AND c.column_name = fk.column_name
                WHERE 
                    c.table_schema = 'public'
                ORDER BY 
                    c.table_name, 
                    c.ordinal_position;
            """)

            cursor.execute(schema_query)
            results = cursor.fetchall()
            current_table = None
            for row in results:
                (table, column, data_type, max_length, nullable, default, description, 
                 constraint_type, foreign_table, foreign_column) = row
                if table != current_table:
                    if print_results: print(f"Table: {table}")
                    current_table = table
                if table not in schema_info:
                    schema_info[table] = {'columns': {}}
                
                type_info = f"{data_type}"
                if max_length:
                    type_info += f"({max_length})"
                
                constraints = []
                if constraint_type == 'PRIMARY KEY':
                    constraints.append("PK")
                elif constraint_type == 'FOREIGN KEY':
                    constraints.append(f"FK -> {foreign_table}({foreign_column})")
                if nullable == 'NO':
                    constraints.append("NOT NULL")
                if default:
                    constraints.append(f"DEFAULT {default}")
                
                schema_info[table]['columns'][column] = {
                    'type': type_info,
                    'constraints': constraints,
                    'description': description,
                }
                
                if print_results:
                    print(f"  Column: {column}")
                    print(f"  Type: {type_info}")
                    if constraints:
                        print(f"  Constraints: {', '.join(constraints)}")
                    if description:
                        print(f"  Description: {description}")
                    print("  ---")
    finally:
        conn.close()
    
    return schema_info

def extract_table_statistics(database, print_results=False):
    """
    Extracts table statistics from the specified database, including distribution boundaries.
    
    Args:
    database (str): The name of the database to extract statistics from.
    print_results (bool): Whether to print the results to console.
    
    Returns:
    dict: A dictionary containing table statistics.
    """
    conn_params = get_db_connection_params()
    conn_params['dbname'] = database
    statistics = {}
    
    try:
        conn = psycopg2.connect(**conn_params)
        with conn.cursor() as cursor:
            # Query to get basic table statistics
            basic_stats_query = """
            SELECT
                schemaname AS db_name,
                relname AS table_name,
                n_live_tup AS row_count,
                pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
                pg_size_pretty(pg_table_size(relid)) AS table_size,
                pg_size_pretty(pg_indexes_size(relid)) AS index_size
            FROM
                pg_stat_user_tables
            ORDER BY
                n_live_tup DESC;
            """
            
            cursor.execute(basic_stats_query)
            basic_results = cursor.fetchall()
            
            for row in basic_results:
                schema_name, table_name, row_count, total_size, table_size, index_size = row
                statistics[table_name] = {
                    'row_count': row_count,
                    'total_size': total_size,
                    'table_size': table_size,
                    'index_size': index_size,
                    'columns': {}
                }
                
                # Query to get column statistics including distribution boundaries
                column_stats_query = f"""
                SELECT
                    a.attname AS column_name,
                    pg_stats.n_distinct,
                    pg_stats.null_frac,
                    pg_stats.avg_width,
                    pg_stats.n_distinct,
                    pg_stats.correlation,
                    pg_stats.most_common_vals,
                    pg_stats.most_common_freqs,
                    pg_stats.histogram_bounds
                FROM
                    pg_stats
                JOIN
                    pg_attribute a ON pg_stats.attname = a.attname
                WHERE
                    pg_stats.schemaname = %s AND pg_stats.tablename = %s
                    AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY
                    a.attnum;
                """
                
                cursor.execute(column_stats_query, (schema_name, table_name,))
                column_results = cursor.fetchall()
                
                for col_row in column_results:
                    (col_name, n_distinct, null_frac, avg_width, n_distinct, 
                     correlation, most_common_vals, most_common_freqs, histogram_bounds) = col_row
                    
                    statistics[table_name]['columns'][col_name] = {
                        'n_distinct': n_distinct,
                        'null_fraction': null_frac,
                        'avg_width': avg_width,
                        'correlation': correlation,
                        'most_common_values': most_common_vals,
                        # 'most_common_frequencies': most_common_freqs,
                        'histogram_bounds': histogram_bounds
                    }
                
                if print_results:
                    print(f"Table: {table_name}")
                    print(f"  Row Count: {row_count}")
                    print(f"  Total Size: {total_size}")
                    print(f"  Table Size: {table_size}")
                    print(f"  Index Size: {index_size}")
                    print("  Columns:")
                    for col_name, col_stats in statistics[table_name]['columns'].items():
                        print(f"    {col_name}:")
                        print(f"      Distinct Values: {col_stats['n_distinct']}")
                        print(f"      Null Fraction: {col_stats['null_fraction']}")
                        print(f"      Average Width: {col_stats['avg_width']}")
                        print(f"      Correlation: {col_stats['correlation']}")
                        print(f"      Most Common Values: {col_stats['most_common_values']}")
                        print(f"      Most Common Frequencies: {col_stats['most_common_frequencies']}")
                        print(f"      Histogram Bounds: {col_stats['histogram_bounds']}")
                    print("  ---")
    
    finally:
        conn.close()
    
    return statistics



def scan_databases(filter_builtin_databases=True, print_results=False) -> dict:
    """
    Extracts schema and statistics for all databases, merging the information.
    Returns a dictionary containing the combined information for all databases.
    """
    def merge(source, destination):
        for key, value in source.items():
            if isinstance(value, dict):
                # get node or create one
                node = destination.setdefault(key, {})
                merge(value, node)
            else:
                destination[key] = value

        return destination
    
    all_database_info = {}
    databases = extract_databases(filter_builtin_databases=filter_builtin_databases, print_results=False)
    
    for db_name in databases:
        db_info = {'name': db_name}
        
        # Extract schema
        schema = extract_schema(database=db_name, print_results=False)
        # Extract statistics
        statistics = extract_table_statistics(database=db_name, print_results=False)
        
        # Merge schema and statistics
        tables = {}
        for table_name in schema.keys():
            table_info = schema.get(table_name, {}).copy()
            merge(statistics.get(table_name, {}), table_info)
            tables[table_name] = table_info
        
        db_info['tables'] = tables
        all_database_info[db_name] = db_info
    
    if print_results:
        print(json.dumps(all_database_info, indent=2))
    
    return all_database_info

def execute_sql_query(database: str, query: str) -> dict:
    """
    Executes a SQL query on the specified database and returns the result or the execution error.

    Args:
    database (str): The name of the database to connect to.
    query (str): The SQL query to execute.

    Returns:
    dict: A dictionary containing either the query result or an error message.
    """
    try:
        conn_params = get_db_connection_params()
        conn_params['dbname'] = database
        
        conn = psycopg2.connect(**conn_params)
        
        with conn.cursor() as cur:
            cur.execute(query)
            
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                results = cur.fetchall()
                return {
                    "success": True,
                    "columns": columns,
                    "data": results
                }
            else:
                conn.commit()
                return {
                    "success": True,
                    "message": f"Query executed successfully. Rows affected: {cur.rowcount}"
                }
    
    except psycopg2.Error as e:
        return {
            "success": False,
            "error": str(e)
        }
    
    finally:
        if conn:
            conn.close()

