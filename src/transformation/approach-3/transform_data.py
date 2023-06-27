import logging
import time
from typing import Dict

import pyspark.sql.functions as F
import requests  # type: ignore

# import schedule
from apscheduler.schedulers.background import BackgroundScheduler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


def load_data_frames(
    end_points_list: list, data_frames: Dict[str, DataFrame], spark: SparkSession
) -> Dict[str, DataFrame]:
    url_link = "https://xloop-dummy.herokuapp.com/"
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

                if variable_name == "appointment_df":
                    data_frames[variable_name] = data_frames[
                        variable_name
                    ].withColumnRenamed("appointment_time", "updated")

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
        if end_point == "appointment":
            data_frames[f"{end_point}_df"] = data_frames[
                f"{end_point}_df"
            ].withColumnRenamed("updated", "appointment_time")

    return data_frames


def df_merge(data_frames: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    # Perform joins on the dfs to create a merged df
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
    # filter the active and confirmed  price log
    merged_df = merged_df.filter(
        (merged_df["is_active"] == "true") & (merged_df["confirmed"] == "true")
    )

    # Select specific columns from the merged df
    merged_df = merged_df.select(
        "councillor_id",
        "patient_id",
        "rating",
        "amount_in_pkr",
        "category",
        "appointment_time",
        "appointment_id",
    )

    # Removes rows with any missing values
    cleaned_missing_values_df = merged_df.na.drop()

    # Cleaning Duplicate Records
    cleaned_df = cleaned_missing_values_df.dropDuplicates()

    return cleaned_df


def calculate_appointment_gap_duration(sorted_data_df: DataFrame) -> DataFrame:
    # Explode the array column to create a row for each element in the array
    df_exploded = sorted_data_df.select(
        "councillor_id", "patient_id", "category", "appointment_time"
    )

    # Define a window spec for ordering the rows based on the timestamp
    window_spec = Window.partitionBy("councillor_id", "patient_id", "category").orderBy(
        "appointment_time"
    )

    # Calculate the duration between consecutive timestamps
    df_with_duration = df_exploded.withColumn(
        "previous_appointment_time", F.lag(F.col("appointment_time")).over(window_spec)
    )

    # Calculate the duration in seconds
    df_with_duration = df_with_duration.withColumn(
        "duration",
        (
            F.unix_timestamp(F.col("appointment_time").cast("timestamp"))
            - F.unix_timestamp(F.col("previous_appointment_time").cast("timestamp"))
        ).cast("int"),
    )

    # Calculate the duration in days
    df_with_duration = df_with_duration.withColumn(
        "duration_days", F.col("duration") / 86400
    )

    return df_with_duration


def treatment_numbers(again_sorted_df: DataFrame, spark: SparkSession) -> DataFrame:
    my_list = []
    count_number = 1
    for_loop_1st_iteration = True

    # Iterate over the values
    for next_row in again_sorted_df.collect():
        if for_loop_1st_iteration:
            temp_row = next_row
            for_loop_1st_iteration = False
        if (
            next_row["treatment_start_status"] == 2
            and next_row["councillor_id"] == temp_row["councillor_id"]
            and next_row["patient_id"] == temp_row["patient_id"]
            and next_row["category"] == temp_row["category"]
        ):
            count_number = count_number + 1
            my_list.append(count_number)

        elif next_row["treatment_start_status"] == 2:
            my_list.append(count_number)

        elif (
            next_row["councillor_id"] == temp_row["councillor_id"]
            and next_row["patient_id"] == temp_row["patient_id"]
            and next_row["category"] == temp_row["category"]
        ):
            my_list.append(count_number)

        else:
            count_number = 1
            my_list.append(count_number)
        temp_row = next_row

    # Create a list of tuples
    data_tuples = [(value,) for value in my_list]

    # Create the df
    data_df = data_tuples
    new_df = spark.createDataFrame(data_df, ["specific_treatment_number"])

    # Add a sequential index column
    rdd_with_index = new_df.rdd.zipWithIndex()
    treatment_numbers_df = spark.createDataFrame(
        rdd_with_index, ["Data", "Index"]
    ).select("Data.*", "Index")

    return treatment_numbers_df


def success_rate(combined_table: DataFrame) -> DataFrame:
    # Group the combined_table df by "councillor_id", "patient_id", "category", and
    # "specific_treatment_number" and calculate the success rate as a percentage
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
    # Group the combined_table and calculate the treatments duration by multiplying the number of appointments by 30
    treatments_duration_df = (
        combined_table.groupBy(
            "councillor_id", "patient_id", "category", "specific_treatment_number"
        )
        .agg(F.count("*").alias("councillor_appointments"))
        .withColumn("treatments_duration", F.col("councillor_appointments") * F.lit(30))
    )

    # Calculate the average total duration of appointments for each councillor
    avg_total_duration_of_appointments_df = treatments_duration_df.groupBy(
        "councillor_id"
    ).agg(F.mean("treatments_duration").alias("avg_time_spent"))

    return avg_total_duration_of_appointments_df


def cost(combined_table: DataFrame) -> DataFrame:
    # Calculate the count of appointments by "councillor_id", "patient_id", "category" and "specific_treatment_number"
    concillor_appointments_df = (
        combined_table.groupBy(
            "councillor_id", "patient_id", "category", "specific_treatment_number"
        )
        .agg(F.count("*").alias("concillor_appointments"))
        .join(combined_table, "councillor_id")
    )

    # Calculate the treatment cost by multiplying the "concillor_appointments" and "amount_in_pkr" columns
    treatment_cost_df = concillor_appointments_df.withColumn(
        "treatment_cost", F.col("concillor_appointments") * F.col("amount_in_pkr")
    )

    # Calculate the average of the "treatment_cost"
    avg_treatment_cost_df = treatment_cost_df.groupBy("councillor_id").agg(
        (F.round(F.mean("treatment_cost"))).alias("avg_cost_spent")
    )

    return avg_treatment_cost_df


def appointments_per_treatment(combined_table: DataFrame) -> DataFrame:
    # Calculate the count of appointments by "councillor_id", "patient_id", "category" and "specific_treatment_number"
    concillor_appointments_df = combined_table.groupBy(
        "councillor_id", "patient_id", "category", "specific_treatment_number"
    ).agg(F.count("*").alias("concillor_appointments"))

    # Calculate the average no of appointments per treatment
    avg_concillor_appointments_df = concillor_appointments_df.groupBy(
        "councillor_id"
    ).agg((F.round(F.mean("concillor_appointments"))).alias("avg_appointments"))

    return avg_concillor_appointments_df


def total_councillor_treatments(combined_table: DataFrame) -> DataFrame:
    # Create a temporary df to hold the combined table
    temp_df = combined_table

    # Select the desired columns from the combined table
    temp_df = temp_df.select(
        "councillor_id", "patient_id", "category", "specific_treatment_number"
    )

    # Remove any duplicate rows
    temp_df = temp_df.dropDuplicates()

    # Group the temporary df by 'councillor_id' and calculate the total treatments using count aggregation
    total_treatments_df = temp_df.groupBy("councillor_id").agg(
        F.count("*").alias("total_treatments")
    )

    return total_treatments_df


def appointment_status(cleaned_df: DataFrame) -> DataFrame:
    # Add a new column "appointment_status" based on the "rating" column
    appointment_status_df = cleaned_df.withColumn(
        "appointment_status",
        F.when(F.col("rating") >= 4, "successful").otherwise("unsuccessful"),
    )

    return appointment_status_df


def sort_data(sort_df: DataFrame) -> DataFrame:
    # Order the df by "councillor_id", "patient_id", "category", and "appointment_time" in ascending order
    sorted_data_df = sort_df.orderBy(
        "councillor_id", "patient_id", "category", "appointment_time"
    )

    return sorted_data_df


def treatment_start_status(gap_duration_df: DataFrame) -> DataFrame:
    # Add new column that contain new treatment start status if gap between appointment greater tha 14 days
    treatment_start_status_df = gap_duration_df.withColumn(
        "treatment_start_status", F.when(F.col("duration_days") > 14, 2).otherwise(1)
    )

    return treatment_start_status_df


def sort_on_category(treatment_start_status_df: DataFrame) -> DataFrame:
    # Order the df treatment_start_status_df by "councillor_id", "patient_id", and "category" in ascending order
    sort_on_category_df = treatment_start_status_df.orderBy(
        "councillor_id", "patient_id", "category"
    )

    return sort_on_category_df


def specific_treatment_numbers(
    treatment_start_status_df: DataFrame, specific_treatment_numbers_df: DataFrame
) -> DataFrame:
    # Add a new index column
    treatment_start_status_df = treatment_start_status_df.withColumn(
        "Index", F.monotonically_increasing_id()
    )

    # Join the treatment_start_status_df with specific_treatment_numbers_df on the 'Index' column
    specific_treatment_df = treatment_start_status_df.join(
        specific_treatment_numbers_df, "Index"
    )

    # Select the desired columns in the resulting df
    specific_treatment_df = specific_treatment_df.select(
        "councillor_id",
        "patient_id",
        "category",
        "specific_treatment_number",
        "appointment_time",
    )

    # Drop duplicate rows based on all columns
    specific_treatment_df = specific_treatment_df.dropDuplicates()

    return specific_treatment_df


def rename_columns(appointment_status_df: DataFrame) -> DataFrame:
    column_names = ["councillor_id", "patient_id", "category", "appointment_time"]

    for name in column_names:
        appointment_status_df = appointment_status_df.withColumnRenamed(
            name, f"{name}_app"
        )

    return appointment_status_df


def final_table(
    specific_treatment_df: DataFrame, appointment_status_df: DataFrame
) -> DataFrame:
    # Define the join condition
    join_condition = (
        (specific_treatment_df.councillor_id == appointment_status_df.councillor_id_app)
        & (specific_treatment_df.patient_id == appointment_status_df.patient_id_app)
        & (specific_treatment_df.category == appointment_status_df.category_app)
        & (
            specific_treatment_df.appointment_time
            == appointment_status_df.appointment_time_app
        )
    )

    # Perform an inner join using the join condition
    combined_table = specific_treatment_df.join(
        appointment_status_df, join_condition, "inner"
    )

    # Select the desired columns from the combined table
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
    data_frames = rename_columns_names(end_points_list, data_frames)
    merged_df = df_merge(data_frames)
    cleaned_df = data_preprocessing(merged_df)
    appointment_status_df = appointment_status(cleaned_df)
    sorted_data_df = sort_data(appointment_status_df)
    # Apply the appointment_gap_duration function to the add
    # duration_days column that contain duration between
    # appointment gaps
    appointment_gap_duration = calculate_appointment_gap_duration(sorted_data_df)
    treatment_start_status_df = treatment_start_status(appointment_gap_duration)
    sorted_on_category_df = sort_on_category(treatment_start_status_df)
    # Call the treatment_numbers function for adding new column that contains treatment number
    specific_treatment_numbers_df = treatment_numbers(sorted_on_category_df, spark)
    specific_treatment_df = specific_treatment_numbers(
        treatment_start_status_df, specific_treatment_numbers_df
    )
    appointment_status_df = rename_columns(appointment_status_df)
    combined_table = final_table(specific_treatment_df, appointment_status_df)
    outcome_data = outcome_prediction(combined_table)

    return outcome_data


def results_db(outcome_data: DataFrame) -> None:
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
        "report",
    ]

    # Initialize SparkSession
    spark = SparkSession.builder.getOrCreate()
    data_frames = load_data_frames(end_points_list, data_frame, spark)
    outcome_data = transformation(end_points_list, data_frames, spark)
    # results_db(outcome_data)
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
