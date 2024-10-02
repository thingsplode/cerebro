import os
import argparse
import importlib
from src.connectors.pgres import scan_databases
from os.path import join, dirname
import streamlit as st
import pandas as pd
from dotenv import load_dotenv


dotenv_path = join(dirname(__file__), '.env')
print(f'dotenv_path: {dotenv_path}')
load_dotenv(dotenv_path)
pgres = importlib.import_module('src.connectors.pgres')
import logging

# Initialize logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger('src').setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Logging initialized in main.py")

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
def main():
    parser = argparse.ArgumentParser(description="Database management tool")
    parser.add_argument("connector", nargs="?", choices=["connector"], help="Specify the connector type")
    parser.add_argument("db_type", nargs="?", choices=["pgsql"], help="Specify the database type")
    parser.add_argument("action", nargs="?", choices=["scan"], help="Specify the action to perform")
    parser.add_argument("database", nargs="?", choices=["dvdrental"], help="Specify the database to scan")
    args = parser.parse_args()
    if args.connector == "connector" and args.db_type == "pgsql" and args.action == "scan":
        scan_databases(filter_builtin_databases=False, print_results=True)
    else:
        # Run the default Streamlit app if no command-line arguments are provided
        st.set_page_config(page_title="Data Insight Generator", layout="wide")

if __name__ == "__main__":
    main()