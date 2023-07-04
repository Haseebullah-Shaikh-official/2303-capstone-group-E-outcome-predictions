import sys
import unittest

from pyspark.sql import SparkSession

from src.transformation.approach_3.transform_data import (
    appointment_status,
    calculate_appointment_gap_duration,
    data_preprocessing,
    df_merge,
    duplicate_dataframes,
    final_table,
    load_data_frames,
    outcome_prediction,
    rename_columns,
    rename_columns_names,
    sort_by_category,
    sort_by_time,
    specific_treatment_numbers,
    treatment_numbers,
    treatment_start_status,
)

sys.path.append(
    "/home/muhammadhuzaifawaseem/Desktop/capstone_git/2303-capstone-group-E-outcome-predictions"
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

        self.renamed_data_frames = rename_columns_names(
            self.end_points_list, self.data_frames, self.spark
        )
        self.merged_df = df_merge(self.renamed_data_frames)
        self.councilor_df = self.merged_df.filter(self.merged_df["councillor_id"] == 50)
        self.duplicates_df = self.councilor_df.unionAll(self.councilor_df)
        self.cleaned_df = data_preprocessing(self.duplicates_df)
        self.appointment_status_df = appointment_status(self.cleaned_df)
        self.sort_df = sort_by_time(self.appointment_status_df)
        self.appointment_gap_duration = calculate_appointment_gap_duration(self.sort_df)
        self.treatment_start_status_df = treatment_start_status(
            self.appointment_gap_duration
        )
        self.category_sorted_df = sort_by_category(self.treatment_start_status_df)
        self.specific_treatment_numbers_df = treatment_numbers(
            self.category_sorted_df, self.spark
        )
        self.specific_treatment_df = specific_treatment_numbers(
            self.treatment_start_status_df, self.specific_treatment_numbers_df
        )
        self.appointment_status_df = rename_columns(self.appointment_status_df)
        self.combined_table = final_table(
            self.specific_treatment_df, self.appointment_status_df
        )
        self.outcome_data = outcome_prediction(self.combined_table)

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
        assert "rating" in self.renamed_data_frames["rating_df"].columns
        assert "councillor_id" in self.renamed_data_frames["councillor_df"].columns

    def test_merged_df(self):
        # Check if the merged DataFrame has the expected columns
        expected_columns = [
            "patient_id",
            "councillor_id",
            "appointment_id",
            "created",
            "description",
            "specialization",
            "updated",
            "user_id",
            "created",
            "patient_councillor_id",
            "updated",
            "availability_id",
            "confirmed",
            "created",
            "appointment_time",
            "created",
            "rating_id",
            "note",
            "updated",
            "rating",
            "amount_in_pkr",
            "created",
            "price_log_id",
            "is_active",
            "payment_method",
            "updated",
            "category",
            "created",
            "report_id",
            "patient_form_link",
            "updated",
        ]

        assert all(col in self.merged_df.columns for col in expected_columns)

    def test_data_processing(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "rating",
            "amount_in_pkr",
            "category",
            "appointment_time",
            "appointment_id",
        ]
        expected_data = [
            (50, 5832, 4, 120, "Anxiety", "2023-04-22T08:55:48.087Z", 4019),
            (50, 5832, 4, 120, "Depression", "2022-08-06T20:20:43.104Z", 6811),
            (50, 3121, 1, 120, "Anxiety", "2023-05-17T09:37:47.601Z", 5157),
            (50, 3121, 4, 120, "Anxiety", "2023-05-17T06:33:23.382Z", 2336),
            (50, 5832, 4, 120, "Anxiety", "2022-08-06T20:20:43.104Z", 6811),
            (50, 5832, 1, 120, "Anxiety", "2023-05-08T03:32:08.891Z", 209),
            (50, 3121, 1, 120, "Anxiety", "2023-06-04T00:05:35.096Z", 3005),
            (50, 5832, 4, 120, "Depression", "2023-04-22T08:55:48.087Z", 4019),
            (50, 5832, 1, 120, "Depression", "2023-05-08T03:32:08.891Z", 209),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.cleaned_df.collect()

    def test_appointment_status(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "rating",
            "amount_in_pkr",
            "category",
            "appointment_time",
            "appointment_id",
            "appointment_status",
        ]
        expected_data = [
            (
                50,
                5832,
                4,
                120,
                "Anxiety",
                "2023-04-22T08:55:48.087Z",
                4019,
                "successful",
            ),
            (
                50,
                5832,
                4,
                120,
                "Depression",
                "2022-08-06T20:20:43.104Z",
                6811,
                "successful",
            ),
            (
                50,
                3121,
                1,
                120,
                "Anxiety",
                "2023-05-17T09:37:47.601Z",
                5157,
                "unsuccessful",
            ),
            (
                50,
                3121,
                4,
                120,
                "Anxiety",
                "2023-05-17T06:33:23.382Z",
                2336,
                "successful",
            ),
            (
                50,
                5832,
                4,
                120,
                "Anxiety",
                "2022-08-06T20:20:43.104Z",
                6811,
                "successful",
            ),
            (
                50,
                5832,
                1,
                120,
                "Anxiety",
                "2023-05-08T03:32:08.891Z",
                209,
                "unsuccessful",
            ),
            (
                50,
                3121,
                1,
                120,
                "Anxiety",
                "2023-06-04T00:05:35.096Z",
                3005,
                "unsuccessful",
            ),
            (
                50,
                5832,
                4,
                120,
                "Depression",
                "2023-04-22T08:55:48.087Z",
                4019,
                "successful",
            ),
            (
                50,
                5832,
                1,
                120,
                "Depression",
                "2023-05-08T03:32:08.891Z",
                209,
                "unsuccessful",
            ),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.appointment_status_df.collect()

    def test_sort_by_time(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "rating",
            "amount_in_pkr",
            "category",
            "appointment_time",
            "appointment_id",
            "appointment_status",
        ]

        expected_data = [
            (
                50,
                3121,
                4,
                120,
                "Anxiety",
                "2023-05-17T06:33:23.382Z",
                2336,
                "successful",
            ),
            (
                50,
                3121,
                1,
                120,
                "Anxiety",
                "2023-05-17T09:37:47.601Z",
                5157,
                "unsuccessful",
            ),
            (
                50,
                3121,
                1,
                120,
                "Anxiety",
                "2023-06-04T00:05:35.096Z",
                3005,
                "unsuccessful",
            ),
            (
                50,
                5832,
                4,
                120,
                "Anxiety",
                "2022-08-06T20:20:43.104Z",
                6811,
                "successful",
            ),
            (
                50,
                5832,
                4,
                120,
                "Anxiety",
                "2023-04-22T08:55:48.087Z",
                4019,
                "successful",
            ),
            (
                50,
                5832,
                1,
                120,
                "Anxiety",
                "2023-05-08T03:32:08.891Z",
                209,
                "unsuccessful",
            ),
            (
                50,
                5832,
                4,
                120,
                "Depression",
                "2022-08-06T20:20:43.104Z",
                6811,
                "successful",
            ),
            (
                50,
                5832,
                4,
                120,
                "Depression",
                "2023-04-22T08:55:48.087Z",
                4019,
                "successful",
            ),
            (
                50,
                5832,
                1,
                120,
                "Depression",
                "2023-05-08T03:32:08.891Z",
                209,
                "unsuccessful",
            ),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.sort_df.collect()

    def test_calculate_appointment_gap_duration(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "category",
            "appointment_time",
            "previous_appointment_time",
            "duration",
            "duration_days",
        ]

        expected_data = [
            (50, 3121, "Anxiety", "2023-05-17T06:33:23.382Z", None, None, None),
            (
                50,
                3121,
                "Anxiety",
                "2023-05-17T09:37:47.601Z",
                "2023-05-17T06:33:23.382Z",
                11064,
                0.12805555555555556,
            ),
            (
                50,
                3121,
                "Anxiety",
                "2023-06-04T00:05:35.096Z",
                "2023-05-17T09:37:47.601Z",
                1520868,
                17.60263888888889,
            ),
            (50, 5832, "Anxiety", "2022-08-06T20:20:43.104Z", None, None, None),
            (
                50,
                5832,
                "Anxiety",
                "2023-04-22T08:55:48.087Z",
                "2022-08-06T20:20:43.104Z",
                22336505,
                258.5243634259259,
            ),
            (
                50,
                5832,
                "Anxiety",
                "2023-05-08T03:32:08.891Z",
                "2023-04-22T08:55:48.087Z",
                1362980,
                15.775231481481482,
            ),
            (50, 5832, "Depression", "2022-08-06T20:20:43.104Z", None, None, None),
            (
                50,
                5832,
                "Depression",
                "2023-04-22T08:55:48.087Z",
                "2022-08-06T20:20:43.104Z",
                22336505,
                258.5243634259259,
            ),
            (
                50,
                5832,
                "Depression",
                "2023-05-08T03:32:08.891Z",
                "2023-04-22T08:55:48.087Z",
                1362980,
                15.775231481481482,
            ),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.appointment_gap_duration.collect()

    def test_treatment_start_status(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "category",
            "appointment_time",
            "previous_appointment_time",
            "duration",
            "duration_days",
            "treatment_start_status",
        ]

        expected_data = [
            (50, 3121, "Anxiety", "2023-05-17T06:33:23.382Z", None, None, None, 1),
            (
                50,
                3121,
                "Anxiety",
                "2023-05-17T09:37:47.601Z",
                "2023-05-17T06:33:23.382Z",
                11064,
                0.12805555555555556,
                1,
            ),
            (
                50,
                3121,
                "Anxiety",
                "2023-06-04T00:05:35.096Z",
                "2023-05-17T09:37:47.601Z",
                1520868,
                17.60263888888889,
                2,
            ),
            (50, 5832, "Anxiety", "2022-08-06T20:20:43.104Z", None, None, None, 1),
            (
                50,
                5832,
                "Anxiety",
                "2023-04-22T08:55:48.087Z",
                "2022-08-06T20:20:43.104Z",
                22336505,
                258.5243634259259,
                2,
            ),
            (
                50,
                5832,
                "Anxiety",
                "2023-05-08T03:32:08.891Z",
                "2023-04-22T08:55:48.087Z",
                1362980,
                15.775231481481482,
                2,
            ),
            (50, 5832, "Depression", "2022-08-06T20:20:43.104Z", None, None, None, 1),
            (
                50,
                5832,
                "Depression",
                "2023-04-22T08:55:48.087Z",
                "2022-08-06T20:20:43.104Z",
                22336505,
                258.5243634259259,
                2,
            ),
            (
                50,
                5832,
                "Depression",
                "2023-05-08T03:32:08.891Z",
                "2023-04-22T08:55:48.087Z",
                1362980,
                15.775231481481482,
                2,
            ),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.treatment_start_status_df.collect()

    def test_treatment_numbers(self):
        expected_columns = ["specific_treatment_number", "Index"]

        expected_data = [
            (1, 0),
            (1, 1),
            (2, 2),
            (1, 3),
            (2, 4),
            (3, 5),
            (1, 6),
            (2, 7),
            (3, 8),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.specific_treatment_numbers_df.collect()

    def test_specific_treatment_numbers(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "category",
            "specific_treatment_number",
            "appointment_time",
        ]

        expected_data = [
            (50, 5832, "Anxiety", 2, "2023-04-22T08:55:48.087Z"),
            (50, 5832, "Anxiety", 1, "2022-08-06T20:20:43.104Z"),
            (50, 3121, "Anxiety", 2, "2023-06-04T00:05:35.096Z"),
            (50, 5832, "Depression", 2, "2023-04-22T08:55:48.087Z"),
            (50, 5832, "Depression", 3, "2023-05-08T03:32:08.891Z"),
            (50, 3121, "Anxiety", 1, "2023-05-17T06:33:23.382Z"),
            (50, 5832, "Depression", 1, "2022-08-06T20:20:43.104Z"),
            (50, 3121, "Anxiety", 1, "2023-05-17T09:37:47.601Z"),
            (50, 5832, "Anxiety", 3, "2023-05-08T03:32:08.891Z"),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.specific_treatment_df.collect()

    def test_rename_columns(self):
        # Check if the column renaming is applied correctly
        assert "councillor_id_app" in self.appointment_status_df.columns
        assert "patient_id_app" in self.appointment_status_df.columns
        assert "category_app" in self.appointment_status_df.columns
        assert "appointment_time_app" in self.appointment_status_df.columns

    def test_final_table(self):
        expected_columns = [
            "councillor_id",
            "patient_id",
            "category",
            "specific_treatment_number",
            "appointment_status",
            "amount_in_pkr",
        ]

        expected_data = [
            (50, 5832, "Anxiety", 2, "successful", 120),
            (50, 5832, "Anxiety", 1, "successful", 120),
            (50, 3121, "Anxiety", 2, "unsuccessful", 120),
            (50, 5832, "Depression", 2, "successful", 120),
            (50, 5832, "Depression", 3, "unsuccessful", 120),
            (50, 3121, "Anxiety", 1, "successful", 120),
            (50, 5832, "Depression", 1, "successful", 120),
            (50, 3121, "Anxiety", 1, "unsuccessful", 120),
            (50, 5832, "Anxiety", 3, "unsuccessful", 120),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.combined_table.collect()

    def test_outcome_prediction_mean(self):
        expected_columns = [
            "councillor_id",
            "success_rate",
            "avg_time_spent",
            "avg_cost_spent",
            "avg_appointments",
            "total_treatments",
        ]

        expected_data = [(50, 56.25, 33.75, 135.0, 1.0, 8)]
        expected_df = self.spark.createDataFrame(expected_data, expected_columns)
        assert expected_df.collect() == self.outcome_data.collect()


if __name__ == "__main__":
    unittest.main()
