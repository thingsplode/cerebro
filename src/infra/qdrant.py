import os
import uuid
import atexit
import logging
from qdrant_client import QdrantClient
from qdrant_client.http import models
from sentence_transformers import SentenceTransformer
from src.utils import create_schema_text

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize the SentenceTransformer model
model = SentenceTransformer('all-MiniLM-L6-v2')


def initialize_qdrant():
    """
    Initialize the Qdrant client and create the initial index if it doesn't exist.
    """
    qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
    client = QdrantClient(url=qdrant_url)
    
    collection_name = "schema_embeddings"
    
    # Check if the collection exists, if not, create it
    collections = client.get_collections().collections
    if not any(collection.name == collection_name for collection in collections):
        client.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE)
        )
    
    return client, collection_name

def cleanup_qdrant():
    """
    Gracefully release resources and close the connection to Qdrant when terminating the application.
    """
    global client
    if client:
        try:
            # Close the client connection
            client.close()
            logger.info("Qdrant client connection closed successfully.")
        except Exception as e:
            logger.error(f"Error while closing Qdrant client connection: {e}")
        finally:
            # Set the client to None to ensure it's not used after closing
            client = None

client, collection_name = initialize_qdrant()
atexit.register(cleanup_qdrant)

def search_schema_embeddings(user_query, limit=5):
    query_embedding = model.encode(user_query)
    search_result = client.search(
        collection_name=collection_name,
        query_vector=query_embedding.tolist(),
        limit=limit  # Adjust this number based on how many results you want
    )
    return search_result

def retrieve_index_ids_by_payload(payload_filter, limit=100):
    """
    Retrieve index IDs from Qdrant based on payload values.

    :param payload_filter: A dictionary specifying the payload filter conditions
    :param limit: Maximum number of results to return (default: 100)
    :return: A list of index IDs matching the payload filter
    """
    try:
        # Construct the filter based on the provided payload conditions
        filter_conditions = models.Filter(
            must=[
                models.FieldCondition(
                    key=key,
                    match=models.MatchValue(value=value)
                ) for key, value in payload_filter.items()
            ]
        )

        # Perform the search using the constructed filter
        search_result = client.scroll(
            collection_name=collection_name,
            scroll_filter=filter_conditions,
            limit=limit,
            with_payload=False,
            with_vectors=False
        )

        # Extract the index IDs from the search result
        index_ids = [point.id for point in search_result[0]]

        return index_ids

    except Exception as e:
        logger.error(f"Error retrieving index IDs by payload: {e}")
        return []


def create_and_store_schema_embeddings(db_schemas):
    """
    Create schema embeddings from schema info and store them in the Qdrant vector database.
    
    :param client: Initialized QdrantClient
    :param collection_name: Name of the collection to store embeddings
    :param schema_info: Dictionary containing schema information
    """
    for db_name, db_info in db_schemas.items():
        for table_name, table_info in db_info['tables'].items():
            # Create a string representation of the schema
            schema_text = create_schema_text(db_name, table_name, table_info)
            
            # Generate embedding
            embedding = model.encode(schema_text).tolist()
            existing_ids = retrieve_index_ids_by_payload({"database": db_name, "table": table_name})
            q_id = existing_ids[0] if len(existing_ids) > 0 else str(uuid.uuid4())
            if len(existing_ids) > 0:
                logger.debug(f"Updating {db_name}.{table_name} in Qdrant")
            else:
                logger.debug(f"Inserting {db_name}.{table_name} into Qdrant")
            # Store in Qdrant
            client.upsert(
                collection_name=collection_name,
                points=[
                    models.PointStruct(
                        id=q_id,
                        vector=embedding,
                        payload={"database": db_name, "table": table_name, "schema": table_info}
                    )
                ]
            )
