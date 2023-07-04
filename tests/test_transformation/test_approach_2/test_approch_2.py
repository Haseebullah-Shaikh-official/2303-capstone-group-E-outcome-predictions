import sys
import unittest

from pyspark.sql import SparkSession

from src.transformation.approach_2.transform_data import (
    data_processing,
    df_merge,
    df_rename,
    duplicate_dataframes,
    load_data_frames,
    outcome_prediction,
    outcome_prediction_mean,
)

sys.path.append(
    "/home/syedmuhammadhammadirshad/Desktop/emertus_project/project_e_git/2303-capstone-group-E-outcome-predictions"
)


class TransformDataTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        # Create SparkSession for testing
        self.spark = (
            SparkSession.builder.appName("unittest-pyspark")
            .master("local[2]")
            .getOrCreate()
        )

        self.end_points_list = [
            "appointment",
            "patient_councillor",
            "price_log",
            "councillor",
            "rating",
            "report",
        ]
        self.data_frame = {}
        self.data_frames = load_data_frames(
            self.end_points_list, self.data_frame, self.spark
        )
        self.renamed_data_frames = df_rename(self.data_frames)
        self.merged_df = df_merge(self.renamed_data_frames)
        self.councilor_df = self.merged_df.filter(
            self.merged_df["councillor_id"] == 1360
        )
        self.df_treatment = data_processing(self.councilor_df)
        self.data_df = outcome_prediction(self.df_treatment)
        self.final_join = outcome_prediction_mean(self.df_treatment)

    @classmethod
    def tearDownClass(self):
        # Stop SparkSession
        self.spark.stop()

    def test_duplicate_dataframes(self):
        # Create a dummy dataset
        dfs = {
            "patient_councillor_df": self.spark.createDataFrame(
                [(10, 14), (12, 15), (13, 60)], ["councillor_id", "patient_id"]
            ),
            "appointment_df": self.spark.createDataFrame(
                [(70, 10), (18, 110), (19, 12)], ["appointment_id", "patient_id"]
            ),
        }

        # Call the function to get the duplicated dataframes
        duplicates = duplicate_dataframes(dfs, self.spark)

        # Check if the duplicated dataframes have the same column names as the original dataframes
        assert (
            duplicates["patient_councillor_df"].columns
            == dfs["patient_councillor_df"].columns
        )
        assert duplicates["appointment_df"].columns == dfs["appointment_df"].columns

        # Check if the duplicated dataframes have the same content as the original dataframes
        assert (
            duplicates["patient_councillor_df"].collect()
            == dfs["patient_councillor_df"].collect()
        )
        assert duplicates["appointment_df"].collect() == dfs["appointment_df"].collect()

    def test_rename_dfs(self):
        # Check if the column renaming is applied correctly
        assert "appointment_id" in self.renamed_data_frames["appointment_df"].columns
        assert (
            "patient_councillor_id"
            in self.renamed_data_frames["patient_councillor_df"].columns
        )
        assert "price_log_id" in self.renamed_data_frames["price_log_df"].columns
        assert "rating_id" in self.renamed_data_frames["rating_df"].columns
        assert "rating_value" in self.renamed_data_frames["rating_df"].columns
        assert "councillor_id" in self.renamed_data_frames["councillor_df"].columns

    def test_merged_df(self):
        # Check if the merged DataFrame has the expected columns
        expected_columns = [
            "councillor_id",
            "patient_id",
            "rating_id",
            "rating_value",
            "appointment_confirmed",
            "amount_in_pkr",
            "category",
            "appointment_updated",
        ]

        assert all(col in self.merged_df.columns for col in expected_columns)

    def test_data_processing(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "rating_value",
            "amount_in_pkr",
            "category",
            "appointment_updated",
            "success_status",
            "appointment_number",
            "treatment",
        ]
        expected_data = [
            (
                1360,
                2967,
                2,
                2210,
                "Anxiety",
                "2023-06-04T02:48:56.458Z",
                "unsuccessful",
                1,
                1,
            ),
            (
                1360,
                2967,
                2,
                2210,
                "Depression",
                "2023-06-04T02:48:56.458Z",
                "unsuccessful",
                1,
                1,
            ),
            (
                1360,
                6272,
                5,
                2210,
                "Depression",
                "2023-05-20T04:11:29.678Z",
                "successful",
                1,
                1,
            ),
            (
                1360,
                6272,
                2,
                2210,
                "Depression",
                "2023-05-20T04:11:29.678Z",
                "unsuccessful",
                2,
                1,
            ),
            (
                1360,
                6272,
                4,
                2210,
                "Depression",
                "2023-05-20T04:11:29.678Z",
                "successful",
                3,
                1,
            ),
        ]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.df_treatment.collect()

    def test_outcome_prediction(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "category",
            "treatment",
            "successful_appointment",
            "total_appointment",
            "cost_pkr",
            "total_time_spent",
            "success_rate",
        ]

        expected_data = [
            (1360, 2967, "Anxiety", 1, 0, 1, 2210, 30, 0.0),
            (1360, 2967, "Depression", 1, 0, 1, 2210, 30, 0.0),
            (1360, 6272, "Depression", 1, 2, 3, 6630, 90, 66.67),
        ]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.data_df.collect()

    def test_outcome_prediction_mean(self):
        expected_columns = [
            "councillor_id",
            "success_rate",
            "avg_time_spent",
            "avg_cost_spent",
            "avg_appointments",
            "total_treatments",
        ]

        expected_data = [(1360, 22.22, 50.0, 3683.33, 1.67, 3)]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.final_join.collect()


if __name__ == "__main__":
    unittest.main()
