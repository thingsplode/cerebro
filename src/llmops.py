import requests
import logging
from os.path import join
from src.utils import read_and_prepare_prompt
from src import PROJECT_FOLDER
logger = logging.getLogger(__name__)
assets = join(PROJECT_FOLDER, "assets")

def generate_embedding_with_ollama(text: str, model: str = "all-minilm-l6-v2") -> list:
    """
    Generate embeddings for SQL DDL operations using Ollama.

    Args:
    text (str): The SQL DDL text to generate embeddings for.
    model (str): The name of the Ollama model to use. Defaults to "all-minilm-l6-v2".

    Returns:
    list: The generated embedding as a list of floats.
    """
    url = "http://localhost:11434/api/embeddings"
    
    payload = {
        "model": model,
        "prompt": text
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        data = response.json()
        embedding = data.get('embedding')
        
        if embedding is None:
            raise ValueError("No embedding found in the response")
        
        return embedding
    except requests.RequestException as e:
        logger.error(f"Error while generating embedding with Ollama: {e}")
        raise e
    except (KeyError, ValueError) as e:
        logger.error(f"Error processing Ollama response: {e}")
        raise e

def generate_sql_with_ollama(user_question, relevant_schema, all_schema=None, previous_conversation=None):
    """
    Generate SQL using a local Ollama deployment with a custom prompt.

    Args:
    user_question (str): The user's natural language question.
    schema_info (str): The relevant schema information.

    Returns:
    str: The generated SQL query.
    """
    global assets
    system_prompt = read_and_prepare_prompt(join(assets, "generate_sql_system.prompt"))

    prompt = read_and_prepare_prompt(join(assets, "generate_sql.prompt"), user_question=user_question, relevant_schema=relevant_schema, all_schema=all_schema)
    print(f"Prompt: {prompt}")
    return prompt_llm(prompt, system_prompt=system_prompt, context=previous_conversation, model="llama3.1")
    # return process_user_query(prompt, system_prompt=system_prompt, model="sqlcoder")


def improve_prompt(user_query, model="llama3.1"):
    global assets
    prompt = read_and_prepare_prompt(join(assets, "query_improvement.prompt"), user_query=user_query)
    response = prompt_llm(prompt, model=model)
    logger.info(f"Improved query: {response}")
    return response

def prompt_llm(prompt, system_prompt=None, context=None, model='llama3.1'):
    ollama_url = "http://localhost:11434/api/generate"
    payload = {
        "model": model,
        "prompt": prompt,
        "system": system_prompt,
        "stream": False
    }
    if context:
        payload.update(context = context)

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