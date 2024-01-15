# Function to create a custom table in a database
def create_custom_table(connection, table_name):
    try:
        # Create a cursor object using the connection
        with connection.cursor() as cursor:
            # SQL statement to create a table with specified name if it doesn't exist
            # The table has three columns: id (primary key), column1, and column2
            # All columns are of type VARCHAR(255)
            table_creation_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id VARCHAR(255) PRIMARY KEY,
                    column1 VARCHAR(255),
                    column2 VARCHAR(255)
                    -- Add more columns as needed
                );
            """
            # Execute the SQL statement
            cursor.execute(table_creation_sql)
        # Commit the transaction
        connection.commit()
        # Print a success message
        print(f"Table '{table_name}' created successfully!")

    except Exception as e:
        # Print an error message if something goes wrong
        print(f"Error creating table: {e}")

# Function to list all tables in a database
def list_tables(connection):
    try:
        # Create a cursor object using the connection
        with connection.cursor() as cursor:
            # SQL statement to list all tables
            list_tables_sql = """
                SHOW TABLES;
            """
            # Execute the SQL statement
            cursor.execute(list_tables_sql)
            # Fetch all rows from the result of the SQL statement
            # and extract the first column (table name) from each row
            existing_tables = [row[0] for row in cursor.fetchall()]
        # Return the list of table names
        return existing_tables

    except Exception as e:
        # Print an error message if something goes wrong
        print(f"Error listing tables: {e}")
        # Return an empty list
        return []