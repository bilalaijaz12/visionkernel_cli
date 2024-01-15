# Import necessary libraries
import pandas as pd
import mysql.connector

# Function to map pandas data types to MySQL data types
def get_mysql_data_type(pandas_dtype):
    """Maps pandas data types to MySQL data types."""
    if pandas_dtype.name.startswith('int'):
        return 'INT'  # MySQL integer type
    elif pandas_dtype.name.startswith('float'):
        return 'FLOAT'  # or 'DOUBLE' for higher precision
    elif pandas_dtype.name.startswith('datetime'):
        return 'DATETIME'  # MySQL datetime type
    elif pandas_dtype.name.startswith('bool'):
        return 'BOOLEAN'
    else:
        return 'VARCHAR(255)'  # Default to VARCHAR for other types

# Function to create a MySQL table based on the structure of an Excel file
def auto_create_table_from_excel(connection, table_name, excel_file_path, sheet_name):
    try:
        # Read Excel file into a Pandas DataFrame
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

        # Start creating the SQL statement
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("

        # Generate column definitions
        column_definitions = []
        for col in df.columns:
            # Format column name for SQL (replace spaces, etc.)
            formatted_col = col.replace(" ", "_").lower()
            col_data_type = get_mysql_data_type(df[col].dtype)
            column_definitions.append(f"{formatted_col} {col_data_type}")

        # Combine column definitions and complete SQL statement
        create_table_sql += ', '.join(column_definitions) + ");"

        # Execute the SQL statement to create a new table
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
            connection.commit()

        print(f"Table '{table_name}' created successfully.")

    except Exception as e:
        print(f"Error creating table: {e}")

# Function to upload data from an Excel file to a MySQL table
def upload_excel_data(connection, table_name, excel_file_path, sheet_name):
    try:
        # Read Excel file into a Pandas DataFrame
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name)

        # Convert NaN values to None
        df = df.where(pd.notna(df), None)

        # Format column names (convert to lowercase) and create SQL statement
        formatted_columns = [f'"{col.lower()}"' for col in df.columns]
        columns = ', '.join(formatted_columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

        # Create a list of tuples from the DataFrame
        records = [tuple(row) for row in df.to_records(index=False)]

        with connection.cursor() as cursor:
            # Execute the SQL query for insertion
            cursor.executemany(insert_query, records)

        # Commit the transaction
        connection.commit()

        print(f"Data uploaded successfully to table '{table_name}'!")

    except Exception as e:
        print(f"Error uploading data: {e}")