import psycopg2
from psycopg2 import sql
import os
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

def extract_databases(filter_builtin_databases=True, print_results=False):
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

def extract_schema(database, print_results=False):
    """
    Extract schema information for the specified database.
    """
    conn_params = get_db_connection_params(database=database)
    conn = psycopg2.connect(**conn_params)
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

            # Process and print results
            current_table = None
            for row in results:
                (table, column, data_type, max_length, nullable, default, description, 
                 constraint_type, foreign_table, foreign_column) = row    
                if table != current_table:
                    if current_table is not None and print_results:
                        print(f"Current table: {current_table}")
                    if print_results:
                        print(f"Table: {table}")
                    current_table = table
                
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
                
                constraint_info = ", ".join(constraints)
                
                if print_results:
                    print(f"  - {column}: {type_info}")
                    if constraint_info:
                        print(f"    Constraints: {constraint_info}")
                    if description:
                        print(f"    Description: {description}")
    finally:
        conn.close()

def extract_table_statistics(database_name, print_results=False):
    """
    Extract basic statistics from each table and column in the specified database.
    """
    conn_params = get_db_connection_params(database=database_name)
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cursor:
            stats_query = """
            SELECT 
                schemaname,
                relname AS table_name,
                n_live_tup AS row_count,
                pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
                pg_size_pretty(pg_table_size(relid)) AS table_size,
                pg_size_pretty(pg_indexes_size(relid)) AS index_size,
                (SELECT array_agg(a.attname || ': ' || 
                    pg_stats.most_common_vals::text || ' (' || 
                    pg_stats.most_common_freqs::text || ')')
                FROM pg_stats
                JOIN pg_attribute a ON a.attname = pg_stats.attname
                WHERE pg_stats.schemaname = s.schemaname
                AND pg_stats.tablename = s.relname
                AND a.attrelid = s.relid) AS column_stats
            FROM pg_stat_user_tables s
            WHERE schemaname = 'public'
            ORDER BY n_live_tup DESC;
            """
            
            cursor.execute(stats_query)
            results = cursor.fetchall()
            
            # Process and print results
            for row in results:
                (schema, table, row_count, total_size, table_size, index_size, column_stats) = row
                
                if print_results:
                    print(f"\nTable: {table}")
                    print(f"  Schema: {schema}")
                    print(f"  Row count: {row_count}")
                    print(f"  Total size: {total_size}")
                    print(f"  Table size: {table_size}")
                    print(f"  Index size: {index_size}")
                
                if column_stats and print_results:
                    print("  Column statistics:")
                    for stat in column_stats:
                        print(f"    {stat}")
    finally:
        conn.close()
