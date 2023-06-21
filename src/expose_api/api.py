import psycopg2
import uvicorn
from fastapi import FastAPI, HTTPException
from psycopg2 import errors

api = FastAPI()
query = 'SELECT * FROM result WHERE "councillor_id" = %s'


@api.get("/{id}")
def get_data(id: int):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="outcome_prediction",
            user="user",
            password="password",
        )

        # Creating a cursor
        cur = conn.cursor()

        # Execute the SQL query with the specified ID
        cur.execute(query, (id,))

        # Fetch the row
        row = cur.fetchone()

        # Close the cursor and connection
        cur.close()
        conn.close()

        if row is None:
            # Raise an HTTPException with a 404 status code if the row is not found
            raise HTTPException(status_code=404, detail="Data not found")

        # Get column names
        columns = [desc[0] for desc in cur.description]

        # Create a dictionary with column names as keys and row values
        data = {column: value for column, value in zip(columns, row)}

        # Return the fetched data
        return data

    except psycopg2.OperationalError as error:
        if isinstance(error, errors.UndefinedTable):
            # Handle the case when the table does not exist
            raise HTTPException(status_code=404, detail="Table not found")
        elif isinstance(error, errors.DatabaseError):
            # Handle the case when the database does not exist
            raise HTTPException(status_code=404, detail="Database not found")
        else:
            # Handle other operational errors
            raise HTTPException(status_code=500, detail=str(error))

    except psycopg2.ProgrammingError as error:
        # Handle programming errors (e.g., syntax error, invalid query)
        raise HTTPException(status_code=400, detail=str(error))

    except Exception as error:
        # Handle other specific exceptions
        raise HTTPException(status_code=500, detail="An error occurred") from error


if __name__ == "__main__":
    uvicorn.run("api:api", host="0.0.0.0", port=8000)
