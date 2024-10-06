import os
import traceback
import json
import argparse
import logging
from src.connectors.pgres import scan_databases
from os.path import join, dirname
import streamlit as st
import json
from tabulate import tabulate
from dotenv import load_dotenv
from src import set_project_folder
from src.infra.qdrant import create_and_store_schema_embeddings
from src.infra import fetch_relevant_tables_ddl
from src.utils import table_info_to_ddl, save_db_info, load_all_db_info, extract_sql_from_markdown
from src.connectors.pgres import scan_databases, execute_sql_query
from src.llmops import generate_sql_with_ollama, improve_prompt, generate_refined_sql

project_folder = dirname(__file__)
set_project_folder(project_folder)
# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('src').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug(f'Project folder: {project_folder}')
load_dotenv(join(project_folder, '.env'))

# def main():
#     st.title('Text to Insight Application')

#     # Sidebar settings
#     st.sidebar.title('Settings')
#     if st.sidebar.checkbox('Show Settings'):
#         st.sidebar.subheader('Data Sources')
#         data_source = st.sidebar.text_input('Data Source Connection String', '')
#         st.sidebar.subheader('LLM Connection')
#         llm_endpoint = st.sidebar.text_input('LLM Endpoint', 'http://localhost:port/ollama_endpoint')

#     # Scan data sources and build data catalog
#     data_catalog = scan_data_sources()

#     # User question input
#     question = st.text_input('Ask a question about your data:', '')

#     if question:
#         # Retrieve relevant schemas
#         relevant_tables = retrieve_schemas(question)
#         st.write('Relevant tables:', ', '.join(relevant_tables))

#         # Load sample data based on relevant tables
#         if relevant_tables:
#             if 'sales_data' in relevant_tables:
#                 data = pd.DataFrame({
#                     'date': pd.date_range(start='2021-01-01', periods=5, freq='D'),
#                     'product': ['A', 'B', 'C', 'A', 'B'],
#                     'sales_amount': [100, 150, 200, 130, 170],
#                     'region': ['North', 'South', 'East', 'West', 'North']
#                 })
#             elif 'customer_data' in relevant_tables:
#                 data = pd.DataFrame({
#                     'customer_id': [1, 2, 3, 4, 5],
#                     'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
#                     'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com'],
#                     'purchase_history': [5, 3, 6, 2, 4]
#                 })
#             else:
#                 st.error('No relevant data found.')
#                 return
#         else:
#             st.error('No relevant data found.')
#             return

#         # Display filter options
#         st.subheader('Filter and Group Options')
#         filter_column = st.selectbox('Select column to filter:', data.columns)
#         unique_values = data[filter_column].unique()
#         selected_values = st.multiselect('Select values to filter:', unique_values, default=unique_values)
#         group_column = st.selectbox('Select column to group by:', data.columns)

#         # Apply filters
#         filtered_data = data[data[filter_column].isin(selected_values)]

#         # Generate charts
#         st.subheader('Generated Chart')
#         numeric_cols = filtered_data.select_dtypes(include=['number', 'float']).columns
#         if group_column in numeric_cols:
#             chart_data = filtered_data.groupby(group_column).mean().reset_index()
#         else:
#             if len(numeric_cols) > 0:
#                 chart_data = filtered_data.groupby(group_column)[numeric_cols].mean().reset_index()
#             else:
#                 st.error('No numeric data available for chart.')
#                 return
#         st.bar_chart(chart_data.set_index(group_column))

#         # Generate insight
#         insight = generate_insight(question, filtered_data)
#         st.subheader('Insight')
#         st.write(insight)

#         # Bookmark option
#         if st.button('Bookmark this Insight'):
#             if 'bookmarks' not in st.session_state:
#                 st.session_state['bookmarks'] = []
#             st.session_state['bookmarks'].append({
#                 'question': question,
#                 'chart_data': chart_data,
#                 'insight': insight
#             })
#             st.success('Insight bookmarked!')

#     # Display bookmarks in sidebar
#     st.sidebar.title('Bookmarks')
#     if 'bookmarks' in st.session_state:
#         for idx, bookmark in enumerate(st.session_state['bookmarks']):
#             with st.sidebar.expander(f'Bookmark {idx+1}'):
#                 st.write('Question:', bookmark['question'])
#                 st.write('Insight:', bookmark['insight'])
#                 st.bar_chart(bookmark['chart_data'].set_index(bookmark['chart_data'].columns[0]))

def execute_user_query(query: str):
    def execute_sql(sql):
        # todo: chnage the database name to the one in the data source
        rsp = execute_sql_query('dvdrental',sql)
        if rsp["success"]:
            if 'message' in rsp:
                print(f"Query executed successfully. Rows affected: {rsp['message']}")
            else:
                table = tabulate(rsp['data'], headers=rsp['columns'], tablefmt='pretty')
                print("Query results:")
                print(table)
        else:
            logger.error(f"Error executing query: {rsp['error']}")
        return rsp
    
    all_db_info = load_all_db_info(project_folder)
    all_schema = list()
    for db_name, db_info in all_db_info.items():
        for table_name, table_info in db_info["tables"].items():
            all_schema.append(table_info_to_ddl(db_name, table_name, table_info))
    improved_query = improve_prompt(query)
    relevant_tables = fetch_relevant_tables_ddl(improved_query)
    full_schema = f"\n---\n".join(all_schema)
    llm_response = generate_sql_with_ollama(improved_query, relevant_tables, full_schema)
    print(f"Generated SQL for query: {llm_response}")
    extracted_sqls = extract_sql_from_markdown(llm_response)
    for sql in extracted_sqls:
        retry_count = 0
        max_retries = 5
        rsp = {"success": False}
        while not rsp["success"] and retry_count < max_retries:
            if retry_count == 0:
                rsp = execute_sql(sql)
            else:
                improved_sql_query = generate_refined_sql(user_query=sql, 
                                                    relevant_schema=relevant_tables, 
                                                    all_schema=full_schema, 
                                                    error_message=rsp["error"])
                rsp = execute_sql(improved_sql_query)
            retry_count += 1
            if retry_count == max_retries and not rsp["success"]:
                logger.error(f"Failed to execute query after {max_retries} attempts.")
def main():
    parser = argparse.ArgumentParser(description="Database management tool")
    subparsers = parser.add_subparsers(dest="action", help="Specify the action to perform")

    # Scan action
    scan_parser = subparsers.add_parser("scan", help="Scan databases")
    scan_parser.add_argument("data_source", choices=["pgsql"], help="Specify the database type")

    # Query action
    query_parser = subparsers.add_parser("query", help="Query databases")
    query_parser.add_argument("query", type=str, help="Specify the query string")

    args = parser.parse_args()
    if args.action == "scan" and args.data_source == "pgsql":
        db_info = scan_databases(filter_builtin_databases=False, print_results=True)
        save_db_info(db_info, project_folder)
        create_and_store_schema_embeddings(db_info)
    elif args.action == "query" and args.query:
        execute_user_query(args.query)
    else:
        # Run the default Streamlit app if no command-line arguments are provided
        st.set_page_config(page_title="Data Insight Generator", layout="wide")
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(traceback.format_exc())
