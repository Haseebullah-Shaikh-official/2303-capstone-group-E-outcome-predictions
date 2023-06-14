import psycopg2
from fastapi import FastAPI, HTTPException

api = FastAPI()


@api.get("/data/{id}")
async def get_data(id: int):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="outcome_prediction",
            user="user",
            password="password"
        )

        # Creating a cursor
        cur = conn.cursor()

        # Execute the SQL query with the specified ID
        cur.execute("SELECT * FROM result WHERE \"councillor_id\" = %s", (id,))
        

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
        return {"data": data}

    except (Exception, psycopg2.Error) as error:
        if str(error) == "":
            return {"error": "id is not available"}
        else:
            return {"error": str(error)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(api, host="0.0.0.0", port=8000)
