from src.infra.qdrant import search_schema_embeddings
from src.utils import create_schema_text, table_info_to_ddl

def fetch_relevant_tables_schema_text(user_question) -> str:
    search_result = search_schema_embeddings(user_question)
    relevant_tables = list()
    for result in search_result:
        # result.score
        text = create_schema_text(result.payload['database'], result.payload['table'], result.payload['schema'])
        relevant_tables.append(text)
    return "\n---\n".join(relevant_tables)

def fetch_relevant_tables_ddl(user_question) -> str:
    search_result = search_schema_embeddings(user_question)
    relevant_tables = list()
    for result in search_result:
        # result.score
        table_ddl = table_info_to_ddl(result.payload['database'], result.payload['table'], result.payload['schema'] )
        relevant_tables.append(table_ddl)
    return "\n".join(relevant_tables)