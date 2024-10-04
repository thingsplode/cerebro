from src.infra.qdrant import search_schema_embeddings
from src.utils import create_schema_text

def fetch_relevant_tables(user_question):
    search_result = search_schema_embeddings(user_question)
    relevant_tables = list()
    for result in search_result:
        # result.score
        text = create_schema_text(result.payload['database'], result.payload['table'], result.payload['schema'])
        relevant_tables.append(text)
    return "\n---\n".join(relevant_tables)