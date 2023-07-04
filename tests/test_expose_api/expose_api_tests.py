import json
import sys
from unittest import TestCase, mock
from fastapi.testclient import TestClient


sys.path.append("/home/hamzaasim/Documents/git capstone repo/2303-capstone-group-E-outcome-predictions")
from src.expose_api import api

# Importing psycopg2 separately for testing 
import psycopg2



class TestData(TestCase):
    
    def test_get_data_200(self):
        # Mock the psycopg2.connect method
        with mock.patch.object(psycopg2, "connect") as mock_connect:
            
            # Mock the database cursor
            mock_cursor = mock_connect.return_value.cursor.return_value

            # define the expected row data
            expected_row = (
                50,
                56.25,
                33.75,
                135.0,
                1.0,
                8
            )

            # Mock the fetchone method to return the expected row
            mock_cursor.fetchone.return_value = expected_row

            # Create a test client using the FastAPI application
            client = TestClient(api.api)

            # Specify the ID for testing
            councillor_id = 50

            # Send a GET request to the API with the specified ID
            response = client.get(f"/{councillor_id}")

            # Assert that the response has a 200 status code
            self.assertEqual(response.status_code, 200)

            # Assert that the database connection and cursor were called correctly
            mock_connect.assert_called_once_with(
                host="postgres",
                port=5432,
                database="outcome_prediction",
                user="user",
                password="password"
            )
            
            # Assert that execute called once with query and councillor id
            query = "SELECT * FROM result WHERE \"councillor_id\" = %s"
            mock_cursor.execute.assert_called_once_with(
                query,
                (councillor_id,)
            )

            # Assert that fetchone called once 
            mock_cursor.fetchone.assert_called_once()

            mock_cursor.close.assert_called_once()
            mock_connect.return_value.close.assert_called_once()

    def test_get_data_not_found(self):
        # Mock the psycopg2.connect method
        with mock.patch.object(psycopg2, "connect") as mock_connect:
            # Mock the database cursor
            mock_cursor = mock_connect.return_value.cursor.return_value

            # Mock the fetchone method to return None
            mock_cursor.fetchone.return_value = None

            # Create a test client using the FastAPI application
            client = TestClient(api.api)

            # Specify a non-exist ID for testing
            councillor_id = 55

            # Send a GET request to the endpoint with the specified ID
            response = client.get(f"/{councillor_id}")

            # Assert that the response has a 404 status code
            self.assertEqual(response.status_code, 404)

            # Assert that the response data matches the expected error message
            expected_data = {
                "detail": "Data not found"
            }
            self.assertEqual(response.json(), expected_data)

            # Assert that the database connection and cursor were called correctly
            mock_connect.assert_called_once_with(
                host="postgres",
                port=5432,
                database="outcome_prediction",
                user="user",
                password="password"
            )
           
            mock_cursor.execute.assert_called_once_with(
                "SELECT * FROM result WHERE \"councillor_id\" = %s",
                (councillor_id,)
            )
            
            mock_cursor.fetchone.assert_called_once()

            # Assert that the cursor and connection were closed
            mock_cursor.close.assert_called_once()
            mock_connect.return_value.close.assert_called_once()

    
    def test_get_data_operational_error(self):
        # Mock the psycopg2.connect method
        with mock.patch.object(psycopg2, "connect") as mock_connect:
            # Set the side effect for the connect method to raise an OperationalError
            mock_connect.side_effect = psycopg2.OperationalError("Database connection failed")

            # Create a test client using the FastAPI application
            client = TestClient(api.api)

            # Specify the ID for testing
            councillor_id = 50

            # Send a GET request to the endpoint with the specified ID
            response = client.get(f"/{councillor_id}")

            # Assert that the response has a 500 status code
            self.assertEqual(response.status_code, 500)

            # Assert that the response data matches the expected error message
            expected_data = {
                "detail": "Database connection failed"
            }
            self.assertEqual(response.json(), expected_data)

            # Assert that the database connection was called correctly
            mock_connect.assert_called_once_with(
                host="postgres",
                port=5432,
                database="outcome_prediction",
                user="user",
                password="password"
            )

            # Assert that the cursor and connection were not called
            mock_connect.return_value.cursor.assert_not_called()
            mock_connect.return_value.close.assert_not_called()

if __name__ == "__main__":
    unittest.main()
