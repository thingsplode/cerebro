import os
import json

def flatten_columns(columns):
    column_texts = list()
    for column_name, column_info in columns.items():
        description = f", Description: {column_info['description']}, " if 'description' in column_info and column_info.get('description') is not None else ''
        constraints = f", Constraints: [{', '.join(column_info['constraints'])}]" if 'constraints' in column_info and column_info.get('constraints') is not None and len(column_info['constraints']) > 0 else ''
        column_text = f"- Name: {column_name}, Type: {column_info['type']}{description} {constraints}"
        column_texts.append(column_text)
    return column_texts

def create_schema_text(db_name, table_name, table_info):
    row_count = f"Row Count: {table_info['row_count']}, " if 'row_count' in table_info and table_info.get('row_count') is not None else ''
    schema_text = f"Database: {db_name}, Table: {table_name}, {row_count} \nColumns: \n{'\n'.join(flatten_columns(table_info['columns']))}"
    return schema_text

def save_db_info(db_info, project_folder):
    """
    Create a data folder if it doesn't exist in the project folder.
    Save the db_info dictionary in separate JSON files per database.
    
    Args:
    db_info (dict): A dictionary containing database information.
    """
    # Create the data folder if it doesn't exist
    data_folder = os.path.join(project_folder, 'data')
    os.makedirs(data_folder, exist_ok=True)
    
    # Iterate through each database in the db_info dictionary
    for db_name, db_data in db_info.items():
        # Create a filename for the database
        filename = f"{db_name}_info.json"
        file_path = os.path.join(data_folder, filename)
        
        # Save the database information to a JSON file
        with open(file_path, 'w') as f:
            json.dump(db_data, f, indent=2)
        
        print(f"Saved database info for {db_name} to {file_path}")

def load_db_info(db_name, project_folder):
    """
    Load the database information from a previously saved JSON file in the data folder.

    Args:
    db_name (str): The name of the database to load information for.
    project_folder (str): The path to the project folder.

    Returns:
    dict: The loaded database information, or None if the file doesn't exist.
    """
    data_folder = os.path.join(project_folder, 'data')
    filename = f"{db_name}_info.json"
    file_path = os.path.join(data_folder, filename)

    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            db_data = json.load(f)
        print(f"Loaded database info for {db_name} from {file_path}")
        return db_data
    else:
        print(f"No saved information found for database {db_name}")
        return None

def load_all_db_info(project_folder):
    """
    Load all database information from previously saved JSON files in the data folder.

    Args:
    project_folder (str): The path to the project folder.

    Returns:
    dict: A dictionary containing all loaded database information, with database names as keys.
    """
    data_folder = os.path.join(project_folder, 'data')
    all_db_info = {}

    if not os.path.exists(data_folder):
        print(f"Data folder not found at {data_folder}")
        return all_db_info

    for filename in os.listdir(data_folder):
        if filename.endswith('_info.json'):
            file_path = os.path.join(data_folder, filename)
            db_name = filename.replace('_info.json', '')

            with open(file_path, 'r') as f:
                db_data = json.load(f)
            
            all_db_info[db_name] = db_data
            print(f"Loaded database info for {db_name} from {file_path}")

    if not all_db_info:
        print("No database information files found in the data folder")
    else:
        print(f"Loaded information for {len(all_db_info)} database(s)")

    return all_db_info

def read_and_prepare_prompt(template_file, **kwargs):
    """
    Reads a text file and replaces template values with provided arguments.

    Args:
    template_file (str): Path to the template file.
    **kwargs: Keyword arguments where keys are template placeholders and values are replacements.

    Returns:
    str: The content of the file with template values replaced.
    """
    with open(template_file, 'r') as file:
        content = file.read()

    for key, value in kwargs.items():
        placeholder = '{' + key + '}'
        content = content.replace(placeholder, str(value))

    return content
    
