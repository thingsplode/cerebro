def flatten_columns(columns):
    column_texts = list()
    for column_name, column_info in columns.items():
        description = f"Description: {column_info['description']}, " if 'description' in column_info and column_info.get('description') is not None else ''
        column_text = f"Column Name: {column_name}, Column Type: {column_info['type']}, {description} Constraints: [{', '.join(column_info['constraints'])}]"
        column_texts.append(column_text)
    return column_texts

def create_schema_text(db_name, table_name, table_info):
    row_count = f"Row Count: {table_info['row_count']}, " if 'row_count' in table_info and table_info.get('row_count') is not None else ''
    schema_text = f"Database: {db_name}, Table: {table_name}, {row_count} Columns: [{'; '.join(flatten_columns(table_info['columns']))}]"
    return schema_text