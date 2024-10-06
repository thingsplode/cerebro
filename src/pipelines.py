from src import PROJECT_FOLDER
from .llmops import improve_prompt, generate_sql_with_ollama, generate_refined_sql
from logging import getLogger
from tabulate import tabulate
from .connectors.pgres import execute_sql_query
from .utils import extract_sql_from_markdown, load_all_db_info, table_info_to_ddl
from .infra import fetch_relevant_tables_ddl



logger = getLogger(__name__)

def execute_user_query_pipleline(query: str):
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
    
    all_db_info = load_all_db_info(PROJECT_FOLDER)
    all_schema = list()
    for db_name, db_info in all_db_info.items():
        for table_name, table_info in db_info["tables"].items():
            all_schema.append(table_info_to_ddl(db_name, table_name, table_info))
    improved_query = improve_prompt(query)
    relevant_tables = fetch_relevant_tables_ddl(improved_query)
    full_schema = f"\n".join(all_schema)
    llm_response = generate_sql_with_ollama(improved_query, relevant_tables, full_schema)
    print(f"Generated SQL for query: {llm_response['response']}")
    extracted_sqls = extract_sql_from_markdown(llm_response['response'])
    for sql in extracted_sqls:
        retry_count = 0
        max_retries = 5
        rsp = {"success": False}
        current_sql = sql
        while not rsp["success"] and retry_count < max_retries:
            if retry_count == 0:
                rsp = execute_sql(current_sql)
            else:
                improved_sql_query_response = generate_refined_sql(user_query=improved_query, 
                                                    initial_sql=current_sql,
                                                    relevant_schema=relevant_tables, 
                                                    all_schema=full_schema, 
                                                    context=llm_response['context'],
                                                    error_message=rsp["error"])
                logger.debug(f"Refined SQL: {improved_sql_query_response['response']}")
                current_sql = extract_sql_from_markdown(improved_sql_query_response['response'])[0]
                rsp = execute_sql(current_sql)
                if not rsp["success"]:
                    logger.error(f"Error executing query: {rsp}")
            retry_count += 1
            if retry_count == max_retries and not rsp["success"]:
                logger.error(f"Failed to execute query after {max_retries} attempts.")