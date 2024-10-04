import os
import requests
import traceback
import json
import argparse
import logging
from src.connectors.pgres import scan_databases
from os.path import join, dirname
import streamlit as st
import json
import pandas as pd
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from src.infra.qdrant import search_schema_embeddings, create_and_store_schema_embeddings
from src.utils import create_schema_text, save_db_info, load_all_db_info, read_and_prepare_prompt
from src.connectors.pgres import scan_databases 
project_folder = dirname(__file__)


# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('src').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.debug(f'Project folder: {project_folder}')
load_dotenv(join(project_folder, '.env'))
assets = join(project_folder, "assets")
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

def process_user_query(prompt, system_prompt=None, model='llama3.1'):
    ollama_url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "system": system_prompt,
        "stream": False
    }
    try:
        response = requests.post(ollama_url, json=payload)
        response.raise_for_status()
        result = response.json()
        return result['response'].strip()
    except requests.RequestException as e:
        logger.error(f"Error calling Ollama API: {e}")
        if isinstance(e, requests.HTTPError):
            logger.error(f"HTTP Status Code: {e.response.status_code}")
            logger.error(f"Response Content: {e.response.text}")
        elif isinstance(e, requests.ConnectionError):
            logger.error("Connection Error: Unable to connect to the Ollama API server")
        elif isinstance(e, requests.Timeout):
            logger.error("Timeout Error: The request to Ollama API timed out")
        else:
            logger.error(f"Unexpected error type: {type(e).__name__}")
        return None



def improve_prompt(user_query, model="llama3.1"):
    prompt = read_and_prepare_prompt(join(assets, "query_improvement.prompt"), user_query=user_query)
    response = process_user_query(prompt, model=model)
    logger.info(f"Improved query: {response}")
    return response

def generate_sql_with_ollama(user_question, relevant_schema, all_schema=None):
    """
    Generate SQL using a local Ollama deployment with a custom prompt.

    Args:
    user_question (str): The user's natural language question.
    schema_info (str): The relevant schema information.

    Returns:
    str: The generated SQL query.
    """
    ollama_url = "http://localhost:11434/api/generate"
    system_prompt = read_and_prepare_prompt(join(assets, "generate_sql_system.prompt"))

    prompt = read_and_prepare_prompt(join(assets, "generate_sql.prompt"), user_question=user_question, relevant_schema=relevant_schema, all_schema=all_schema)
    print(f"Prompt: {prompt}")
    return process_user_query(prompt, system_prompt=system_prompt, model="llama3.1")
    # return process_user_query(prompt, system_prompt=system_prompt, model="sqlcoder")


def fetch_relevant_tables(user_question, compressed_schema=None):
    search_result = search_schema_embeddings(user_question)
    print("Search results:")
    for idx, result in enumerate(search_result, 1):
        print(f"Result {idx}:")
        print(f"  Score: {result.score}")
        print(f"  Payload: {result.payload}")

    relevant_tables = []
    for result in search_result:
        schema_element = {
            'database': result.payload['database'],
            'table': result.payload['table'],
            'relevance': result.score,
            'table_info': result.payload['schema']
        }
        relevant_tables.append(create_schema_text(schema_element["database"], schema_element["table"], schema_element["table_info"]))
        

    # Create a compressed schema representation
    return "\n---\n".join(relevant_tables)

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
        all_db_info = load_all_db_info(project_folder)
        all_schema = list()
        for db_name, db_info in all_db_info.items():
            for table_name, table_info in db_info["tables"].items():
                all_schema.append(create_schema_text(db_name, table_name, table_info))
        improved_query = improve_prompt(args.query)
        relevant_tables = fetch_relevant_tables(improved_query, None) 
        # print(f"Generated SQL for query: {relevant_tables}")
        sql_query = generate_sql_with_ollama(improved_query, relevant_tables, "\n---\n".join(all_schema))
        print(f"Generated SQL for query: {sql_query}")
    else:
        # Run the default Streamlit app if no command-line arguments are provided
        st.set_page_config(page_title="Data Insight Generator", layout="wide")
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(traceback.format_exc())
