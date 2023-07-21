import logging
import os
import time
from typing import Dict

import pyspark.sql.functions as F
import requests  # type: ignore
import schedule
from pyspark.sql import DataFrame, SparkSession


def load_data_frames(
    url_dict: Dict[str, str], data_frames: Dict[str, DataFrame], spark: SparkSession
) -> Dict[str, DataFrame]:
    for key, url in url_dict.items():
        variable_name = key  # Generate a variable name for the df
        response = requests.get(url)  # Send a GET request to the API

        if response.status_code == 200:
            json_data = response.json()
            df = spark.createDataFrame(
                json_data
            )  # Create a Spark df from the JSON data

            # Check if the df already exists in the dictionary
            if variable_name in data_frames:
                logging.info(f"Data frame: {key} already exists, adding new data")
                # Retrieve the maximum timestamp value from the existing df
                max_timestamp = (
                    data_frames[variable_name]
                    .selectExpr("max(created) as max_timestamp")
                    .collect()[0]["max_timestamp"]
                )
                # Filter new data based on the timestamp column
                new_data = df.filter(F.col("created") > max_timestamp)
                if new_data.count() > 0:
                    # Union the new data with the existing df
                    data_frames[variable_name] = data_frames[variable_name].union(
                        new_data
                    )

            else:
                data_frames[variable_name] = df

        else:
            logging.info(f"No data returned for endpoint: {key}")

    return data_frames


def rename_columns_names(data_frames: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    data_frames["councillor_df"] = data_frames["councillor_df"].withColumnRenamed(
        "id", "councillor_id"
    )
    data_frames["patient_councillor_df"] = data_frames[
        "patient_councillor_df"
    ].withColumnRenamed("id", "patient_councillor_id")
    data_frames["patient_councillor_df"] = data_frames[
        "patient_councillor_df"
    ].withColumnRenamed("councillorId", "councillor_id")
    data_frames["patient_councillor_df"] = data_frames[
        "patient_councillor_df"
    ].withColumnRenamed("patientId", "patient_id")
    data_frames["appointment_df"] = data_frames["appointment_df"].withColumnRenamed(
        "id", "appointment_id"
    )
    data_frames["appointment_df"] = data_frames["appointment_df"].withColumnRenamed(
        "patientId", "patient_id"
    )
    data_frames["rating_df"] = data_frames["rating_df"].withColumnRenamed(
        "value", "rating"
    )
    data_frames["rating_df"] = data_frames["rating_df"].withColumnRenamed(
        "appointmentId", "appointment_id"
    )
    data_frames["price_log_df"] = data_frames["price_log_df"].withColumnRenamed(
        "councillorid", "councillor_id"
    )
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
    merged_df = merged_df.drop("updated")
    # filter the active and confirmed  price log
    merged_df = merged_df.filter(
        (merged_df["isactive"] == "true") & (merged_df["confirmed"] == "true")
    )

    # Select specific columns from the merged DataFrame
    merged_df = merged_df.select("councillor_id", "patient_id", "rating", "amountinpkr")

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
    appointment_time = 30
    treatments_duration_df = (
        cleaned_df.groupBy("councillor_id", "patient_id")
        .agg(F.count("*").alias("councillor_appointments"))
        .withColumn(
            "treatments_duration",
            F.col("councillor_appointments") * F.lit(appointment_time),
        )
    )

    # Group treatments_duration_df, calculate average total appointments duration per councillor.
    avg_total_duration_of_appointments_df = treatments_duration_df.groupBy(
        "councillor_id"
    ).agg(F.mean("treatments_duration").alias("avg_duration"))

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
        "treatment_cost", F.col("concillor_appointments") * F.col("amountinpkr")
    )

    # Calculate the average of the "treatment_cost"
    avg_treatment_cost_df = treatment_cost_df.groupBy("councillor_id").agg(
        F.mean("treatment_cost").alias("avg_cost")
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
    ).agg((F.round(F.mean("concillor_appointments"))).alias("avg_appointments"))

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


def transformation(data_frames: DataFrame) -> DataFrame:
    data_frames = rename_columns_names(data_frames)
    merged_df = df_merge(data_frames)
    cleaned_df = data_preprocessing(merged_df)
    outcome_data = outcome_prediction(cleaned_df)
    return outcome_data


def results_db(outcome_data: DataFrame) -> DataFrame:
    host = os.environ.get("POSTGRES_HOST")
    port = os.environ.get("POSTGRES_PORT")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    database = os.environ.get("POSTGRES_DB")
    url = f"jdbc:postgresql://{host}:{port}/{database}"
    table = "result"

    outcome_data.write.format("jdbc").option("url", url).option(
        "dbtable", table
    ).option("user", user).option("password", password).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()


def main(data_frame: Dict[str, DataFrame]) -> None:
    url_dict = {
        "councillor_df": "http://councelorapp-env.eba-mdmsh3sq.us-east-1.elasticbeanstalk.com/counselor/get",
        "appointment_df": "http://appointment.us-west-2.elasticbeanstalk.com/appointments/getall",
        "rating_df": "http://ratingapp-env.eba-f5gxzjhm.us-east-1.elasticbeanstalk.com/rating/all",
        "price_log_df": "http://192.168.4.44:8085/price_log",
        "patient_councillor_df": "http://appointment.us-west-2.elasticbeanstalk.com/patient_councillor",
    }

    # Initialize SparkSession
    spark = SparkSession.builder.getOrCreate()
    data_frames = load_data_frames(url_dict, data_frame, spark)
    outcome_data = transformation(data_frames)
    results_db(outcome_data)
    return logging.info("Results are stored to db.")


def schedule_job() -> None:
    data_frame: Dict[str, DataFrame] = {}
    main(data_frame)
    schedule.every(1).minute.do(main, data_frame)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(
        filename="transform_data.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # Schedule and run the job
    schedule_job()
