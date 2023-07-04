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
    """
    Loads dataframes from API with endpoints and updates the existing data frames.
    """
    url_link = os.environ.get("URL_LINK")
    for end_point in end_points_list:
        variable_name = f"{end_point}_df"  # Generate a variable name for the df
        url = url_link + end_point  # Construct the URL for the API request
        response = requests.get(url)  # Send a GET request to the API

        if response.status_code == 200:
            json_data = response.json()
            d_f = spark.createDataFrame(
                json_data
            )  # Create a Spark df from the JSON data

            # Check if the df already exists in the dictionary
            if variable_name in data_frames:
                logging.info(f"Data frame: {end_point} already exists, adding new data")

                # Retrieve the maximum timestamp value from the existing df
                max_timestamp = (
                    data_frames[variable_name]
                    .selectExpr("max(updated) as max_timestamp")
                    .collect()[0]["max_timestamp"]
                )
                # Filter new data based on the timestamp column
                new_data = d_f.filter(F.col("updated") > max_timestamp)
                if new_data.count() > 0:
                    # Union the new data with the existing df
                    data_frames[variable_name] = data_frames[variable_name].union(
                        new_data
                    )

            else:
                data_frames[variable_name] = d_f

        else:
            logging.info(f"No data returned for endpoint: {end_point}")

    return data_frames


def duplicate_dataframes(
    data_frames: Dict[str, DataFrame], spark: SparkSession
) -> Dict[str, DataFrame]:
    """
    Create duplicate dataframes for transformation
    """
    duplicate_dfs: Dict[str, DataFrame] = {}
    for df_name, dataframe in data_frames.items():
        duplicate_dfs[df_name] = spark.createDataFrame(dataframe.rdd, dataframe.schema)
    return duplicate_dfs


def rename_columns_names(
    end_points_list: list, data_frames: Dict[str, DataFrame], spark: SparkSession
) -> Dict[str, DataFrame]:
    """
    Renames specific columns in each dataframe based on the endpoint.
    """
    data_frames_dup = duplicate_dataframes(data_frames, spark)
    for end_point in end_points_list:
        data_frames_dup[f"{end_point}_df"] = data_frames_dup[
            f"{end_point}_df"
        ].withColumnRenamed("id", f"{end_point}_id")
        if end_point == "rating":
            data_frames_dup[f"{end_point}_df"] = data_frames_dup[
                f"{end_point}_df"
            ].withColumnRenamed("value", "rating")
        if end_point == "appointment":
            data_frames_dup[f"{end_point}_df"] = data_frames_dup[
                f"{end_point}_df"
            ].withColumnRenamed("updated", "appointment_time")

    return data_frames_dup


def df_merge(data_frames: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Performs joins on the dataframes to create a merged data frame.
    """
    merged_df = (
        data_frames["councillor_df"]
        .join(data_frames["patient_councillor_df"], "councillor_id")
        .join(data_frames["appointment_df"], "patient_id")
        .join(data_frames["rating_df"], "appointment_id")
        .join(data_frames["price_log_df"], "councillor_id")
        .join(data_frames["report_df"], "patient_id")
    )

    return merged_df


def data_preprocessing(merged_df: DataFrame) -> DataFrame:
    """
    Preprocesses the merged dataframe by filtering, selecting columns, removing missing values, and removing duplicates.
    """
    merged_df = merged_df.filter(
        (merged_df["is_active"] == "true") & (merged_df["confirmed"] == "true")
    )
    merged_df = merged_df.select(
        "councillor_id",
        "patient_id",
        "rating",
        "amount_in_pkr",
        "category",
        "appointment_time",
        "appointment_id",
    )
    cleaned_missing_values_df = merged_df.na.drop()
    cleaned_df = cleaned_missing_values_df.dropDuplicates()

    return cleaned_df


def calculate_appointment_gap_duration(sorted_df: DataFrame) -> DataFrame:
    """
    Calculates the duration between consecutive appointments for each councillor-patient-category group.
    """
    seconds_per_day = 86400
    df_exploded = sorted_df.select(
        "councillor_id", "patient_id", "category", "appointment_time"
    )
    window_spec = Window.partitionBy("councillor_id", "patient_id", "category").orderBy(
        "appointment_time"
    )
    df_with_duration = df_exploded.withColumn(
        "previous_appointment_time", F.lag(F.col("appointment_time")).over(window_spec)
    )
    df_with_duration = df_with_duration.withColumn(
        "duration",
        (
            F.unix_timestamp(F.col("appointment_time").cast("timestamp"))
            - F.unix_timestamp(F.col("previous_appointment_time").cast("timestamp"))
        ).cast("int"),
    )

    df_with_duration = df_with_duration.withColumn(
        "duration_days", F.col("duration") / seconds_per_day
    )

    return df_with_duration


def treatment_numbers(category_sorted_df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Assign treatment numbers to series of appointment based on 14 days gape.
    """
    my_list = []
    count_number = 1
    prev_row = spark.createDataFrame([], schema=category_sorted_df.schema)
    if not category_sorted_df.rdd.isEmpty():
        prev_row = category_sorted_df.collect()[0]

    for row in category_sorted_df.collect():
        if (
            row["treatment_start_status"] == 2
            and row["councillor_id"] == prev_row["councillor_id"]
            and row["patient_id"] == prev_row["patient_id"]
            and row["category"] == prev_row["category"]
        ):
            count_number += 1
            my_list.append(count_number)

        elif row["treatment_start_status"] == 2:
            my_list.append(count_number)

        elif (
            row["councillor_id"] == prev_row["councillor_id"]
            and row["patient_id"] == prev_row["patient_id"]
            and row["category"] == prev_row["category"]
        ):
            my_list.append(count_number)

        else:
            count_number = 1
            my_list.append(count_number)

        prev_row = row

    data_tuples = [(value,) for value in my_list]

    data_df = data_tuples
    new_df = spark.createDataFrame(data_df, ["specific_treatment_number"])

    rdd_with_index = new_df.rdd.zipWithIndex()
    treatment_numbers_df = spark.createDataFrame(
        rdd_with_index, ["Data", "Index"]
    ).select("Data.*", "Index")

    return treatment_numbers_df


def success_rate(combined_table: DataFrame) -> DataFrame:
    """
    Calculates the average success rate for each councillor.
    """
    success_rate_df = (
        combined_table.groupBy(
            "councillor_id", "patient_id", "category", "specific_treatment_number"
        )
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


def duration(combined_table: DataFrame) -> DataFrame:
    """
    Calculates the average duration (time spent) for each councillor.
    """
    appointment_duration = 30
    treatments_duration_df = (
        combined_table.groupBy(
            "councillor_id", "patient_id", "category", "specific_treatment_number"
        )
        .agg(F.count("*").alias("councillor_appointments"))
        .withColumn(
            "treatments_duration",
            F.col("councillor_appointments") * F.lit(appointment_duration),
        )
    )
    avg_total_duration_of_treatments_df = treatments_duration_df.groupBy(
        "councillor_id"
    ).agg(F.mean("treatments_duration").alias("avg_time_spent"))

    return avg_total_duration_of_treatments_df


def cost(combined_table: DataFrame) -> DataFrame:
    """
    Calculates the average treatment cost for each councillor.
    """
    concillor_appointments_df = (
        combined_table.groupBy(
            "councillor_id", "patient_id", "category", "specific_treatment_number"
        )
        .agg(F.count("*").alias("concillor_appointments"))
        .join(combined_table, "councillor_id")
    )

    treatment_cost_df = concillor_appointments_df.withColumn(
        "treatment_cost", F.col("concillor_appointments") * F.col("amount_in_pkr")
    )

    # Calculate the average of the "treatment_cost"
    avg_treatment_cost_df = treatment_cost_df.groupBy("councillor_id").agg(
        (F.round(F.mean("treatment_cost"))).alias("avg_cost_spent")
    )

    return avg_treatment_cost_df


def appointments_per_treatment(combined_table: DataFrame) -> DataFrame:
    """
    Calculates the average number of appointments per treatment for each councillor.
    """
    concillor_appointments_df = combined_table.groupBy(
        "councillor_id", "patient_id", "category", "specific_treatment_number"
    ).agg(F.count("*").alias("concillor_appointments"))
    avg_concillor_appointments_df = concillor_appointments_df.groupBy(
        "councillor_id"
    ).agg((F.round(F.mean("concillor_appointments"))).alias("avg_appointments"))

    return avg_concillor_appointments_df


def total_councillor_treatments(combined_table: DataFrame) -> DataFrame:
    """
    Calculates the total number of treatments for each councillor.
    """
    temp_df = combined_table
    temp_df = temp_df.select(
        "councillor_id", "patient_id", "category", "specific_treatment_number"
    )

    temp_df = temp_df.dropDuplicates()
    total_treatments_df = temp_df.groupBy("councillor_id").agg(
        F.count("*").alias("total_treatments")
    )

    return total_treatments_df


def appointment_status(cleaned_df: DataFrame) -> DataFrame:
    """
    Adds "appointment_status" based on the "rating" column.
    """
    success_rating = 4
    appointment_status_df = cleaned_df.withColumn(
        "appointment_status",
        F.when(F.col("rating") >= success_rating, "successful").otherwise(
            "unsuccessful"
        ),
    )

    return appointment_status_df


def sort_by_time(sort_df: DataFrame) -> DataFrame:
    """
    Sorts the DataFrame by "councillor_id", "patient_id", "category", and "appointment_time" in ascending order.
    """
    sorted_df = sort_df.orderBy(
        "councillor_id", "patient_id", "category", "appointment_time"
    )

    return sorted_df


def treatment_start_status(gap_duration_df: DataFrame) -> DataFrame:
    """
    triggers the new treatment strated on basis of 14 days gap

    """
    appointment_gap_in_days = 14
    treatment_start_status_df = gap_duration_df.withColumn(
        "treatment_start_status",
        F.when(F.col("duration_days") > appointment_gap_in_days, 2).otherwise(1),
    )

    return treatment_start_status_df


def sort_by_category(treatment_start_status_df: DataFrame) -> DataFrame:
    """
    Sorts the DataFrame based on the "councillor_id", "patient_id", and "category" columns in ascending order.
    """
    sort_on_category_df = treatment_start_status_df.orderBy(
        "councillor_id", "patient_id", "category"
    )

    return sort_on_category_df


def specific_treatment_numbers(
    treatment_start_status_df: DataFrame, specific_treatment_numbers_df: DataFrame
) -> DataFrame:
    """
    Join two dfs on the basis of index
    """
    treatment_start_status_df = treatment_start_status_df.withColumn(
        "Index", F.monotonically_increasing_id()
    )
    specific_treatment_df = treatment_start_status_df.join(
        specific_treatment_numbers_df, "Index"
    )
    specific_treatment_df = specific_treatment_df.select(
        "councillor_id",
        "patient_id",
        "category",
        "specific_treatment_number",
        "appointment_time",
    )
    specific_treatment_df = specific_treatment_df.dropDuplicates()

    return specific_treatment_df


def rename_columns(appointment_status_df: DataFrame) -> DataFrame:
    """
    Renames specific columns in the DataFrame.
    """
    column_names = ["councillor_id", "patient_id", "category", "appointment_time"]

    for name in column_names:
        appointment_status_df = appointment_status_df.withColumnRenamed(
            name, f"{name}_app"
        )

    return appointment_status_df


def final_table(
    specific_treatment_df: DataFrame, appointment_status_df: DataFrame
) -> DataFrame:
    """
    Combines specific treatment data with appointment status and returns required columns.
    """
    join_condition = (
        (specific_treatment_df.councillor_id == appointment_status_df.councillor_id_app)
        & (specific_treatment_df.patient_id == appointment_status_df.patient_id_app)
        & (specific_treatment_df.category == appointment_status_df.category_app)
        & (
            specific_treatment_df.appointment_time
            == appointment_status_df.appointment_time_app
        )
    )

    combined_table = specific_treatment_df.join(
        appointment_status_df, join_condition, "inner"
    )
    combined_table = combined_table.select(
        "councillor_id",
        "patient_id",
        "category",
        "specific_treatment_number",
        "appointment_status",
        "amount_in_pkr",
    )

    return combined_table


def outcome_prediction(combined_table: DataFrame) -> DataFrame:
    """
    Returns outcome predictions based on combined treatment data.
    """
    success_rate_df = success_rate(combined_table)
    duration_df = duration(combined_table)
    cost_df = cost(combined_table)
    appointments_df = appointments_per_treatment(combined_table)
    total_treatments_df = total_councillor_treatments(combined_table)

    # Joining dfs to create an outcomes table
    outcomes_table = (
        success_rate_df.join(duration_df, "councillor_id")
        .join(cost_df, "councillor_id")
        .join(appointments_df, "councillor_id")
        .join(total_treatments_df, "councillor_id")
    )

    return outcomes_table


def transformation(
    end_points_list: list, data_frames: Dict[str, DataFrame], spark: SparkSession
) -> DataFrame:
    """
    Transforms the data with help of defined functions and returns the final results.
    """
    data_frames = rename_columns_names(end_points_list, data_frames, spark)
    merged_df = df_merge(data_frames)
    cleaned_df = data_preprocessing(merged_df)
    appointment_status_df = appointment_status(cleaned_df)
    sorted_df = sort_by_time(appointment_status_df)
    appointment_gap_duration = calculate_appointment_gap_duration(sorted_df)
    treatment_start_status_df = treatment_start_status(appointment_gap_duration)
    category_sorted_df = sort_by_category(treatment_start_status_df)
    specific_treatment_numbers_df = treatment_numbers(category_sorted_df, spark)
    specific_treatment_df = specific_treatment_numbers(
        treatment_start_status_df, specific_treatment_numbers_df
    )
    appointment_status_df = rename_columns(appointment_status_df)
    combined_table = final_table(specific_treatment_df, appointment_status_df)
    outcome_data = outcome_prediction(combined_table)

    return outcome_data


def results_db(outcome_data: DataFrame) -> None:
    """
    Overwrite the final results to a PostgreSQL database table.
    """
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
    """
    Implements the complete ETL pipeline
    """
    end_points_list = [
        "appointment",
        "patient_councillor",
        "price_log",
        "councillor",
        "rating",
        "report",
    ]

    spark = SparkSession.builder.getOrCreate()
    data_frames = load_data_frames(end_points_list, data_frame, spark)
    duplicate_dfs = duplicate_dataframes(data_frames, spark)
    outcome_data = transformation(end_points_list, duplicate_dfs, spark)
    results_db(outcome_data)
    return outcome_data.show()


def schedule_job() -> None:
    """
    Schedule main function (pipeline)
    """
    data_frame: Dict[str, DataFrame] = {}
    scheduler = BackgroundScheduler()
    scheduler.add_job(main, "interval", minutes=2, args=[data_frame])
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
    schedule_job()
