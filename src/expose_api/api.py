import psycopg2
import uvicorn
from fastapi import FastAPI, HTTPException
from psycopg2 import OperationalError

api: FastAPI = FastAPI()
QUERY = 'SELECT * FROM result WHERE "councillor_id" = %s'


@api.get("/{councillor_id}")
def get_data(councillor_id: int) -> dict:
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
        cur.execute(QUERY, (councillor_id,))

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
        data = dict(zip(columns, row))

        # Return the fetched data
        return data

    except OperationalError as error:
        # Handle operational errors related to the database connection and query execution
        raise HTTPException(status_code=500, detail=str(error)) from error


if __name__ == "__main__":
    uvicorn.run("api:api", host="0.0.0.0", port=8000)
