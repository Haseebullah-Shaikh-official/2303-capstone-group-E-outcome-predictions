import psycopg2

# PostgreSQL connection details
host="postgres"
port=5432
database="outcome_prediction"
user="user"
password="password"

# SQL statement to create the table
create_table_query = """
    CREATE TABLE IF NOT EXISTS result (
        councillor_id SERIAL PRIMARY KEY,
        success_rate FLOAT,
        avg_time_spent FLOAT,
        avg_cost_spent FLOAT,
        avg_appointments FLOAT
    );
"""
cursor = None
connection = None

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Execute the create table query
    cursor.execute(create_table_query)
    connection.commit()

    print("Table 'result' created successfully (if it didn't exist already).")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL:", error)

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()
