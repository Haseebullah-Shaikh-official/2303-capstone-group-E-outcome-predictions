import logging
import os
import time
from typing import Dict

import pyspark.sql.functions as F
import requests  # type: ignore
from apscheduler.schedulers.background import BackgroundScheduler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


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
                logging.info(f"Data frame: {end_point} already exists, adding new data")

                if variable_name == "appointment_df":
                    data_frames[variable_name] = data_frames[
                        variable_name
                    ].withColumnRenamed("appointment_updated", "updated")

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


def df_rename(data_frames: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    # Rename the columns with meaning ful names
    data_frames["appointment_df"] = (
        data_frames["appointment_df"]
        .withColumnRenamed("id", "appointment_id")
        .withColumnRenamed("confirmed", "appointment_confirmed")
        .withColumnRenamed("updated", "appointment_updated")
    )

    data_frames["patient_councillor_df"] = data_frames[
        "patient_councillor_df"
    ].withColumnRenamed("id", "patient_councillor_id")

    data_frames["price_log_df"] = (
        data_frames["price_log_df"]
        .withColumnRenamed("id", "price_log_id")
        .withColumnRenamed("is_active", "acc_is_active")
    )

    data_frames["rating_df"] = (
        data_frames["rating_df"]
        .withColumnRenamed("id", "rating_id")
        .withColumnRenamed("value", "rating_value")
    )

    data_frames["councillor_df"] = data_frames["councillor_df"].withColumnRenamed(
        "id", "councillor_id"
    )

    return data_frames


def df_merge(data_frames: Dict[str, DataFrame]) -> DataFrame:
    # Perform joins on the dfs to create a merged df
    merged_df = (
        data_frames["councillor_df"]
        .join(data_frames["patient_councillor_df"], "councillor_id")
        .join(data_frames["appointment_df"], "patient_id")
        .join(data_frames["rating_df"], "appointment_id")
        .join(data_frames["price_log_df"], "councillor_id")
        .join(data_frames["report_df"], "patient_id")
        .select(
            "councillor_id",
            "patient_id",
            "rating_id",
            "rating_value",
            "appointment_confirmed",
            "amount_in_pkr",
            "category",
            "appointment_updated",
        )
        .dropDuplicates()
    )

    return merged_df


def data_processing(merged_df: DataFrame) -> DataFrame:
    # Filter the confirmed appointments and adding success status
    filtered_table = (
        merged_df.select("*")
        .where(merged_df.appointment_confirmed == True)
        .withColumn(
            "success_status",
            F.when(merged_df.rating_value >= 4, "successful").otherwise("unsuccessful"),
        )
    )

    # Partitioned and ordered data, assigned appointment number, and removed unnecessary columns.
    window_spec = Window.partitionBy("patient_id", "category", "councillor_id").orderBy(
        "appointment_updated"
    )
    df = filtered_table.withColumn(
        "appointment_number", F.row_number().over(window_spec)
    ).drop("rating_id", "appointment_confirmed", "acc_is_active")

    # Assign new columns "treatment" by applying ceil function to assign values in multiples of five
    df_treatment = df.withColumn("treatment", F.ceil(F.col("appointment_number") / 5))

    return df_treatment


def outcome_prediction(df_treatment: DataFrame) -> DataFrame:
    data_df = (
        df_treatment.groupBy("councillor_id", "patient_id", "category", "treatment")
        .agg(
            F.sum(
                F.when(F.col("success_status") == "successful", 1).otherwise(0)
            ).alias("successful_appointment"),
            F.count("*").alias("total_appointment"),
            F.sum("amount_in_pkr").alias("cost_pkr"),
        )
        .withColumn("total_time_spent", F.col("total_appointment") * 30)
        .withColumn(
            "success_rate",
            F.round(
                (F.col("successful_appointment") / F.col("total_appointment")) * 100, 2
            ),
        )
    )

    return data_df


def outcome_prediction_mean(df_treatment: DataFrame) -> DataFrame:
    data_df = outcome_prediction(df_treatment)
    data_mean = data_df.groupBy("councillor_id").agg(
        F.round(F.mean("success_rate"), 2).alias("success_rate"),
        F.round(F.mean("total_time_spent"), 2).alias("avg_time_spent"),
        F.round(F.mean("cost_pkr"), 2).alias("avg_cost_spent"),
    )

    appointment_data = df_treatment.groupby(
        "councillor_id", "patient_id", "category", "treatment"
    ).agg(
        F.count("*").alias("total_appointment"),
        F.max("treatment").alias("number_treatment"),
    )

    mean_appointment = appointment_data.groupBy("councillor_id").agg(
        F.round(F.mean("total_appointment"), 2).alias("avg_appointments"),
        F.count("number_treatment").alias("total_treatments"),
    )

    final_join = data_mean.join(mean_appointment, "councillor_id")

    return final_join


def transformation(data_frames: Dict[str, DataFrame]) -> DataFrame:
    renamed_data_frames = df_rename(data_frames)
    merged_df = df_merge(renamed_data_frames)
    processing_df = data_processing(merged_df)
    outcome_data = outcome_prediction_mean(processing_df)
    return outcome_data


def results_db(outcome_data):
    outcome_data.write.format("jdbc").option(
        "url", "jdbc:postgresql://postgres:5432/outcome_prediction"
    ).option("dbtable", "result").option("user", "user").option(
        "password", "password"
    ).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()


def main(data_frame: Dict[str, DataFrame]) -> DataFrame:
    end_points_list = [
        "appointment",
        "patient_councillor",
        "price_log",
        "councillor",
        "rating",
        "report",
    ]

    # Initialize SparkSession
    spark = SparkSession.builder.getOrCreate()
    data_frames = load_data_frames(end_points_list, data_frame, spark)
    outcome_data = transformation(data_frames)
    results_db(outcome_data)
    return outcome_data.show()


def schedule_job() -> None:
    data_frame: Dict[str, DataFrame] = {}
    scheduler = BackgroundScheduler()
    scheduler.add_job(main, "interval", minutes=1, args=[data_frame])
    scheduler.start()

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    logging.basicConfig(
        filename="transform_data.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Schedule and run the job
    schedule_job()
