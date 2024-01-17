import pandas as pd
import mysql.connector
from contextlib import contextmanager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

@contextmanager
def mysql_cursor(connection):
    cursor = connection.cursor(dictionary=True)
    try:
        yield cursor
    finally:
        cursor.close()

def get_mysql_data_type(pandas_dtype):
    if pandas_dtype.name.startswith('int'):
        return 'INT'
    elif pandas_dtype.name.startswith('float'):
        return 'FLOAT'
    elif pandas_dtype.name.startswith('datetime'):
        return 'DATETIME'
    elif pandas_dtype.name.startswith('bool'):
        return 'BOOLEAN'
    else:
        return 'VARCHAR(255)'

def create_table_from_dataframe(connection, table_name, df):
    column_definitions = [f"{col.replace(' ', '_').lower()} {get_mysql_data_type(dtype)}" 
                          for col, dtype in df.dtypes.items()]
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)});"
    with mysql_cursor(connection) as cursor:
        cursor.execute(create_table_sql)
        connection.commit()
    logging.info(f"Table '{table_name}' created successfully.")

def table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}';")
    return cursor.fetchone() is not None

def get_primary_key(cursor, table_name):
    cursor.execute(f"SHOW KEYS FROM {table_name} WHERE Key_name = 'PRIMARY'")
    primary_keys = [row["Column_name"] for row in cursor.fetchall()]
    return primary_keys

def update_database(connection, table_name, df_new, primary_keys):
    with mysql_cursor(connection) as cursor:
        # Fetch existing data
        cursor.execute(f"SELECT * FROM {table_name}")
        df_existing = pd.DataFrame(cursor.fetchall())

        # Set primary key as index for comparison
        df_existing.set_index(primary_keys, inplace=True)
        df_new.set_index(primary_keys, inplace=True)

        # Identify new and modified rows
        df_new_rows = df_new.loc[~df_new.index.isin(df_existing.index)]
        df_modified_rows = df_new.loc[df_new.index.isin(df_existing.index) & (df_new != df_existing).any(axis=1)]

        # Insert new rows
        for index, row in df_new_rows.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))

        # Update modified rows
        for index, row in df_modified_rows.iterrows():
            update_set = ', '.join([f"{col} = %s" for col in row.index])
            update_query = f"UPDATE {table_name} SET {update_set} WHERE {' AND '.join([f'{pk} = %s' for pk in primary_keys])}"
            cursor.execute(update_query, tuple(row.values) + tuple(index))

        connection.commit()

def upload_excel_data(connection, table_name, excel_file_path, sheet_name):
    df_new = pd.read_excel(excel_file_path, sheet_name=sheet_name)
    df_new.fillna(value=pd.NA, inplace=True)

    logging.info(f"Data types in DataFrame: \n{df_new.dtypes}")
    logging.info(f"First few rows of data: \n{df_new.head()}")

    with mysql_cursor(connection) as cursor:
        if not table_exists(cursor, table_name):
            create_table_from_dataframe(connection, table_name, df_new)
        else:
            primary_keys = get_primary_key(cursor, table_name)
            if not primary_keys:
                logging.warning(f"No primary key found for table '{table_name}'.")
                # Handle the case where no primary key is found
            else:
                try:

                    update_database(connection, table_name, df_new, primary_keys)
                except mysql.connector.errors.DataError as err:
                    logging.error(f"Error updating database: {err.msg}")
                    # Handle the case where the data types of the columns in the Excel file don't match the columns in the database
