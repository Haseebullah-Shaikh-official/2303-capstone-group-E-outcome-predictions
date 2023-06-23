import logging
import os
import time
from typing import Dict

import pyspark.sql.functions as F
import requests  # type: ignore
import schedule
from pyspark.sql import DataFrame, SparkSession


def load_data_frames(
    end_points_list: list, data_frames: Dict[str, DataFrame], spark: SparkSession
) -> Dict[str, DataFrame]:
    url_link = os.environ.get("URL_LINK")
    for end_point in end_points_list:
        variable_name = f"{end_point}_df"  # Generate a variable name for the df
        url = url_link + end_point  # Construct the URL for the API request
        response = requests.get(url)  # Send a GET request to the API

        if response.status_code == 200:
            json_data = response.json()
            df = spark.createDataFrame(
                json_data
            )  # Create a Spark df from the JSON data

            # Check if the df already exists in the dictionary
            if variable_name in data_frames:
                # Retrieve the maximum timestamp value from the existing df
                max_timestamp = (
                    data_frames[variable_name]
                    .selectExpr("max(updated) as max_timestamp")
                    .collect()[0]["max_timestamp"]
                )
                # Filter new data based on the timestamp column
                new_data = df.filter(F.col("updated") > max_timestamp)
                if new_data.count() > 0:
                    # Union the new data with the existing df
                    data_frames[variable_name] = data_frames[variable_name].union(
                        new_data
                    )

            else:
                data_frames[variable_name] = df

        else:
            logging.info(f"No data returned for endpoint: {end_point}")

    return data_frames


def rename_columns_names(
    end_points_list: list, data_frames: Dict[str, DataFrame]
) -> Dict[str, DataFrame]:
    for end_point in end_points_list:
        data_frames[f"{end_point}_df"] = data_frames[
            f"{end_point}_df"
        ].withColumnRenamed("id", f"{end_point}_id")
        if end_point == "rating":
            data_frames[f"{end_point}_df"] = data_frames[
                f"{end_point}_df"
            ].withColumnRenamed("value", "rating")
    return data_frames


def df_merge(data_frames: Dict[str, DataFrame]) -> DataFrame:
    # Perform joins on the DataFrames to create a merged DataFrame
    merged_df = (
        data_frames["councillor_df"]
        .join(data_frames["patient_councillor_df"], "councillor_id")
        .join(data_frames["appointment_df"], "patient_id")
        .join(data_frames["rating_df"], "appointment_id")
        .join(data_frames["price_log_df"], "councillor_id")
    )

    return merged_df


def data_preprocessing(merged_df: DataFrame) -> DataFrame:
    # filter the active and confirmed  price log
    merged_df = merged_df.filter(
        (merged_df["is_active"] == "true") & (merged_df["confirmed"] == "true")
    )

    # Select specific columns from the merged DataFrame
    merged_df = merged_df.select(
        "councillor_id", "patient_id", "rating", "amount_in_pkr"
    )

    # Removes rows with any missing values
    cleaned_missing_values_df = merged_df.na.drop()

    # Cleaning Duplicate Records
    cleaned_df = cleaned_missing_values_df.dropDuplicates()

    return cleaned_df


def success_rate(cleaned_df: DataFrame) -> DataFrame:
    # Create a new column "appointment_status" based on the "rating" column
    appointment_status_df = cleaned_df.withColumn(
        "appointment_status",
        F.when(F.col("rating") >= 4, "successful").otherwise("unsuccessful"),
    )

    # Group DataFrame, count successful and total appointments, calculate success rate.
    success_rate_df = (
        appointment_status_df.groupBy("councillor_id", "patient_id")
        .agg(
            F.sum(
                F.when(F.col("appointment_status") == "successful", 1).otherwise(0)
            ).alias("successful_appointment"),
            F.count("*").alias("total_appointment"),
        )
        .withColumn(
            "success_rate",
            F.round(
                (F.col("successful_appointment") / F.col("total_appointment")) * 100, 2
            ),
        )
    )

    # Calculate the average success rate for each councillor
    avg_success_rate_df = success_rate_df.groupBy("councillor_id").agg(
        F.mean("success_rate").alias("success_rate")
    )

    return avg_success_rate_df


def duration(cleaned_df: DataFrame) -> DataFrame:
    # Group cleaned_df, calculate appointments count, multiply by 30 for treatments duration.
    treatments_duration_df = (
        cleaned_df.groupBy("councillor_id", "patient_id")
        .agg(F.count("*").alias("councillor_appointments"))
        .withColumn("treatments_duration", F.col("councillor_appointments") * F.lit(30))
    )

    # Group treatments_duration_df, calculate average total appointments duration per councillor.
    avg_total_duration_of_appointments_df = treatments_duration_df.groupBy(
        "councillor_id"
    ).agg(F.mean("treatments_duration").alias("avg_duration_per_treatment"))

    return avg_total_duration_of_appointments_df


def cost(cleaned_df: DataFrame) -> DataFrame:
    # Calculate the count of appointments by "councillor_id" for each "patient_id"
    concillor_appointments_df = (
        cleaned_df.groupBy("councillor_id", "patient_id")
        .agg(F.count("*").alias("concillor_appointments"))
        .join(cleaned_df, "councillor_id")
    )

    # Calculate the treatment cost by multiplying the "concillor_appointments" and "amount_in_pkr" columns
    treatment_cost_df = concillor_appointments_df.withColumn(
        "treatment_cost", F.col("concillor_appointments") * F.col("amount_in_pkr")
    )

    # Calculate the average of the "treatment_cost"
    avg_treatment_cost_df = treatment_cost_df.groupBy("councillor_id").agg(
        F.mean("treatment_cost").alias("avg_cost_per_treatment")
    )

    return avg_treatment_cost_df


def appointments_per_treatment(cleaned_df: DataFrame) -> DataFrame:
    # Calculate the count of appointments by "councillor_id" for each "patient_id"
    concillor_appointments_df = cleaned_df.groupBy("councillor_id", "patient_id").agg(
        F.count("*").alias("concillor_appointments")
    )

    # Calculate the average no of appointments per treatment
    avg_concillor_appointments_df = concillor_appointments_df.groupBy(
        "councillor_id"
    ).agg(
        (F.round(F.mean("concillor_appointments"))).alias(
            "avg_appointments_per_treatment"
        )
    )

    return avg_concillor_appointments_df


def outcome_prediction(cleaned_df: DataFrame) -> DataFrame:
    success_rate_df = success_rate(cleaned_df)
    duration_df = duration(cleaned_df)
    cost_df = cost(cleaned_df)
    appointments_df = appointments_per_treatment(cleaned_df)

    # Joining dataframes to create an outcomes table
    outcomes_table = (
        success_rate_df.join(duration_df, "councillor_id")
        .join(cost_df, "councillor_id")
        .join(appointments_df, "councillor_id")
    )

    return outcomes_table


def transformation(data_frames: DataFrame, end_points_list: list) -> DataFrame:
    data_frames = rename_columns_names(end_points_list, data_frames)
    merged_df = df_merge(data_frames)
    cleaned_df = data_preprocessing(merged_df)
    outcome_data = outcome_prediction(cleaned_df)
    return outcome_data


def results_db(outcome_data: DataFrame) -> DataFrame:
    outcome_data.write.format("jdbc").option(
        "url", "jdbc:postgresql://postgres:5432/outcome_prediction"
    ).option("dbtable", "result").option("user", "user").option(
        "password", "password"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()


def main(data_frame: Dict[str, DataFrame]) -> None:
    end_points_list = [
        "appointment",
        "patient_councillor",
        "price_log",
        "councillor",
        "rating",
    ]

    # Initialize SparkSession
    spark = SparkSession.builder.getOrCreate()
    data_frames = load_data_frames(end_points_list, data_frame, spark)
    outcome_data = transformation(data_frames, end_points_list)
    results_db(outcome_data)
    return outcome_data.show()


def schedule_job() -> None:
    data_frame: Dict[str, DataFrame] = {}
    schedule.every(1).minutes.do(main, data_frame)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    # Define a list of endpoints and dfs

    logging.basicConfig(
        filename="transform_data.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Schedule and run the job
    schedule_job()
