import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F 
from datetime import datetime
from typing import Dict
import requests
import sys
import os

src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(src_dir)
import src.transformation.approach_1.transform_data as transform_data

def create_dataframes(spark: SparkSession) -> Dict[str, DataFrame]:
    # Define the column names
    appointment_columns = ["id", "created", "updated", "availability_id", "patient_id", "confirmed"]
    rating_columns = ["id", "created", "updated", "appointment_id", "value", "note"]
    councillor_columns = ["id", "created", "updated", "specialization", "description", "user_id"]
    patient_councillor_columns = ["id", "created", "updated", "patient_id", "councillor_id"]
    price_log_columns = ["id", "created", "updated", "amount_in_pkr", "payment_method", "is_active", "councillor_id"]

    # Create the DataFrames with column names and inferSchema=True
    appointment = spark.createDataFrame([
        (2, "2022-11-13T11:07:15.940Z", "2023-05-15T21:58:53.666Z", 7123, 0, False)
    ], schema=appointment_columns)

    rating = spark.createDataFrame([
        (2, "2022-08-29T12:48:02.846Z", "2023-04-17T03:46:37.654Z", 2, 3, "Iste laudantium totam recusandae officia enim ea. Temporibus eos expedita mollitia. Minus atque ipsam odit.")
    ], schema=rating_columns)

    councillor = spark.createDataFrame([
        (0, "2022-09-19T22:43:38.171Z", "2023-06-05T13:56:56.296Z", "Anxiety", "Dolor vitae eius voluptate eaque vero quam. Similique ullam reiciendis cum pariatur iusto omnis. Accusantium molestiae excepturi tempora nesciunt ratione.", 0)
        ], schema=councillor_columns)

    patient_councillor = spark.createDataFrame([
        (2, "2022-10-16T22:51:02.431Z", "2023-05-13T19:35:08.628Z", 0, 0)
    ], schema=patient_councillor_columns)

    price_log = spark.createDataFrame([(0, "2022-12-31T09:14:54.666Z", "2023-05-27T11:17:59.013Z", 2019, "bank_transfer", True, 0 )
    ], schema=price_log_columns)
    # Create a dictionary of DataFrames
    data_frames = {
        'appointment_df': appointment,
        'rating_df': rating,
        'councillor_df': councillor,
        'patient_councillor_df' : patient_councillor,
        'price_log_df': price_log
    }

    return data_frames

def create_cleaned_df(spark: SparkSession) -> DataFrame:
    # Define the  column names and data
    columns = ["councillor_id", "patient_id", "rating", "amount_in_pkr"]
    data = [
        (1360, 6272, 4, 2210),
        (1360, 2967, 2, 2210),
        (1360, 6272, 2, 2210),
        (1360, 6272, 5, 2210)
    ]

    cleaned_df = spark.createDataFrame(data, schema=columns)

    return cleaned_df

class TransformDataTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # Create SparkSession for testing
        self.spark = SparkSession.builder\
        .appName("unittest-pyspark")\
        .master("local[2]")\
        .getOrCreate()

        self.spark_session_mock = MagicMock(spec=SparkSession)
        self.spark_context_mock = self.spark_session_mock.sparkContext
        self.end_points_list = ["appointment", "patient_councillor", "price_log", "councillor", "rating"]

    @classmethod
    def tearDownClass(self):
        # Stop SparkSession
        self.spark.stop()

    def test_load_dataframes_200_response(self):
        with patch.dict(os.environ, {'URL_LINK': 'https://xloop-dummy.herokuapp.com/'}), \
                patch("requests.get") as requests_mock:
            # Mock the response and JSON data
            response_mock = MagicMock()
            response_mock.status_code = 200
            response_mock.json.return_value = [
                {
                    "id": 0,
                    "created": "2022-10-03T02:25:26.383Z",
                    "updated": "2023-01-01T16:18:44.590Z",
                    "availability_id": 2209,
                    "patient_id": 5532,
                    "confirmed": True
                },
                {
                    "id": 1,
                    "created": "2022-09-19T18:33:25.613Z",
                    "updated": "2023-02-06T04:08:21.639Z",
                    "availability_id": 3160,
                    "patient_id": 754,
                    "confirmed": False
                },
            ]
            requests_mock.return_value = response_mock

            data_frames = {}

            # Call the function
            result = transform_data.load_data_frames(self.end_points_list, data_frames, self.spark_session_mock)

            # Assertions
            self.assertEqual(len(result), 5)
            self.assertIn("price_log_df", result)
            self.assertIn("appointment_df", result)
            self.assertIn("patient_councillor_df", result)
            self.assertIn("councillor_df", result)
            self.assertIn("rating_df", result)

    
    def test_load_dataframes_400_response(self):
        with patch.dict(os.environ, {'URL_LINK': 'https://xloop-dummy.herokuapp.com/'}), \
                patch("requests.get") as requests_mock:
            response_mock = MagicMock()
            response_mock.status_code = 404
            requests_mock.return_value = response_mock

            data_frames = {}

            # Call the function
            result = transform_data.load_data_frames(self.end_points_list, data_frames, self.spark_session_mock)

            # Assertions
            self.assertEqual(len(result), 0)

    def test_dataframes_already_exists(self):
        with patch.dict(os.environ, {'URL_LINK': 'https://xloop-dummy.herokuapp.com/'}), \
                patch("requests.get") as requests_mock, \
                patch("pyspark.sql.functions.col") as col_mock:
            response_mock = MagicMock()
            response_mock.status_code = 200
            response_mock.json.return_value = [
                {
                    "id": 0,
                    "created": "2022-10-03T02:25:26.383Z",
                    "updated": "2023-01-01T16:18:44.590Z",
                    "availability_id": 2209,
                    "patient_id": 5532,
                    "confirmed": True
                },
                {
                    "id": 1,
                    "created": "2022-09-19T18:33:25.613Z",
                    "updated": "2023-02-06T04:08:21.639Z",
                    "availability_id": 3160,
                    "patient_id": 754,
                    "confirmed": False
                },
            ]
            requests_mock.return_value = response_mock

            # Define test data
            end_points_list = ["appointment"]
            existing_data = [
                (0, "2022-10-03T02:25:26.383Z", "2023-01-01T16:18:44.590Z", 2209, 5532, True)

            ]
            df = self.spark_session_mock.createDataFrame(existing_data,
                                                         ["id", "created", "updated", "availability_id",
                                                          "patient_id", "confirmed"])

            # Use the real DataFrame in the data_frames dictionary
            data_frames = {"appointment_df": df}

            # Mock the col function
            col_mock.return_value.__gt__.return_value = MagicMock()

            # Set up the _jvm attribute of the SparkContext mock
            self.spark_context_mock._jvm = MagicMock()

            # Mock the count method of the filtered DataFrame
            new_data_mock = MagicMock()
            new_data_mock.count.return_value = 2
            df.filter.return_value = new_data_mock

            # Call the function
            result = transform_data.load_data_frames(end_points_list, data_frames, self.spark_session_mock)

            # Assertions
            self.assertEqual(len(result), 1)
            self.assertIn("appointment_df", result)

            # Verify method calls
            requests_mock.assert_called_once()
            col_mock.assert_called_with("updated")
            df.filter.assert_called_with(col_mock.return_value.__gt__.return_value)
            new_data_mock.count.assert_called_once()

    def test_rename_dfs(self):
        data_frames = create_dataframes(self.spark)
        renamed_data_frames = transform_data.rename_columns_names(self.end_points_list, data_frames)
        
        # Check if the column renaming is applied correctly
        assert "appointment_id" in renamed_data_frames["appointment_df"].columns
        assert "rating_id" in renamed_data_frames["rating_df"].columns
        assert "rating" in renamed_data_frames["rating_df"].columns
        assert "councillor_id" in renamed_data_frames["councillor_df"].columns
        assert "patient_councillor_id" in renamed_data_frames["patient_councillor_df"].columns
        assert "price_log_id" in renamed_data_frames["price_log_df"].columns

    def test_merged_df(self):
        data_frames = create_dataframes(self.spark)
        renamed_data_frames = transform_data.rename_columns_names(self.end_points_list, data_frames)
        self.merged_df = transform_data.df_merge(renamed_data_frames)

        # Check if the merged DataFrame has the expected columns
        expected_columns = ['councillor_id', 'appointment_id', 'patient_id', 'created', 'updated', 'specialization', 
                            'description', 'user_id', 'patient_councillor_id', 'created', 'updated', 'created', 
                            'updated', 'availability_id', 'confirmed', 'rating_id', 'created', 'updated', 'rating', 
                            'note', 'price_log_id', 'created', 'updated', 'amount_in_pkr', 'payment_method', 'is_active']
        assert all(col in self.merged_df.columns for col in expected_columns)

    def test_data_processing(self):
        councillor_1360 = self.spark.read.csv('data/councillor_1360.csv', header=True, inferSchema=True)  
        cleaned_df = transform_data.data_preprocessing(councillor_1360)
        expected_df = create_cleaned_df(self.spark)
        assert expected_df.collect() == cleaned_df.collect()

    def test_success_rate(self):  

        cleaned_df = create_cleaned_df(self.spark)
        avg_success_rate_df = transform_data.success_rate(cleaned_df)

        expected_columns = [
            "councillor_id",
            "success_rate"
        ]

        expected_data = [
            (1360,33.335)
            
        ]
        
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == avg_success_rate_df.collect()

    def test_duration(self):

        cleaned_df = create_cleaned_df(self.spark)
        avg_total_duration_df = transform_data.duration(cleaned_df)

        expected_columns = [
            "councillor_id",
            "avg_duration_per_treatment"
        ]

        expected_data = [
            (1360,60.0)
            
        ]
        
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == avg_total_duration_df.collect()

    def test_cost(self):
        cleaned_df = create_cleaned_df(self.spark)
        avg_treatment_cost_df = transform_data.cost(cleaned_df)
        expected_columns = [
            "councillor_id",
            "avg_cost_per_treatment"
        ]

        expected_data = [
            (1360,4420.0)
            
        ]
        
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == avg_treatment_cost_df.collect()

    def test_appointments_per_treatment(self):
        cleaned_df = create_cleaned_df(self.spark)
        avg_appointments_df = transform_data.appointments_per_treatment(cleaned_df)
        expected_columns = [
            "councillor_id",
            "avg_appointments_per_treatment"
        ]

        expected_data = [
            (1360,2.0)
            
        ]
        
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == avg_appointments_df.collect()

    def test_outcome_prediction(self):
        cleaned_df = create_cleaned_df(self.spark)
        outcomes_table = transform_data.outcome_prediction(cleaned_df)

        expected_columns = [
            "councillor_id",
            "success_rate",
            "avg_duration_per_treatment",
            "avg_cost_per_treatment",
            "avg_appointments_per_treatment"
        ]

        expected_data = [
            (1360, 33.335, 60.0, 4420.0, 2.0)
            
        ]
        
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == outcomes_table.collect()



if __name__ == "__main__":
    unittest.main()
