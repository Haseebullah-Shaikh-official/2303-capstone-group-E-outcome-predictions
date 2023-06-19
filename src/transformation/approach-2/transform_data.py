import json
import time

import requests  # type: ignore
import schedule
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    ceil,
    col,
    count,
    max,
    mean,
    round,
    row_number,
    sum,
    when,
)
from pyspark.sql.window import Window


def load_data():
    # Define a list of endpoints
    end_points_list = [
        "appointment",
        "patient_councillor",
        "price_log",
        "councillor",
        "rating",
        "report",
    ]
    # Create a dictionary to store the dfs
    data_frames = {}

    for end_point in end_points_list:
        variable_name = f"{end_point}_df"  # Generate a variable name for the df
        url = f"https://xloop-dummy.herokuapp.com/{end_point}"  # Construct the URL for the API request
        response = requests.get(url)  # Send a GET request to the API

        if response.status_code == 200:
            json_data = json.loads(response.text)
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
                new_data = df.filter(col("updated") > max_timestamp)
                if new_data.count() > 0:
                    # Union the new data with the existing df
                    data_frames[variable_name] = data_frames[variable_name].union(
                        new_data
                    )

            else:
                data_frames[variable_name] = df

        else:
            print(f"No data returned for endpoint: {end_point}")

    return data_frames


def df_rename(data_frames):
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


def df_merge(data_frames):
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


def data_processing(merged_df):
    # Filter the confirmed appointments and adding success status
    filtered_table = (
        merged_df.select("*")
        .where(merged_df.appointment_confirmed == True)
        .withColumn(
            "success_status",
            when(merged_df.rating_value >= 4, "successful").otherwise("unsuccessful"),
        )
    )

    # Partitioned and ordered data, assigned appointment number, and removed unnecessary columns.
    window_spec = Window.partitionBy("patient_id", "category", "councillor_id").orderBy(
        "appointment_updated"
    )
    df = filtered_table.withColumn(
        "appointment_number", row_number().over(window_spec)
    ).drop("rating_id", "appointment_confirmed", "acc_is_active")

    # Assign new columns "treatment" by applying ceil function to assign values in multiples of five
    df_treatment = df.withColumn("treatment", ceil(col("appointment_number") / 5))

    return df_treatment


def outcome_prediction(df_treatment):
    data = (
        df_treatment.groupBy("councillor_id", "patient_id", "category", "treatment")
        .agg(
            sum(when(col("success_status") == "successful", 1).otherwise(0)).alias(
                "successful_appointment"
            ),
            count("*").alias("total_appointment"),
            sum("amount_in_pkr").alias("cost_pkr"),
        )
        .withColumn("total_time_spent", col("total_appointment") * 30)
        .withColumn(
            "success_rate",
            round((col("successful_appointment") / col("total_appointment")) * 100, 2),
        )
    )

    return data


def outcome_prediction_mean(df_treatment):
    data_df = outcome_prediction(df_treatment)
    data_mean = data_df.groupBy("councillor_id").agg(
        round(mean("success_rate"), 2).alias("success_rate"),
        round(mean("total_time_spent"), 2).alias("avg_time_spent"),
        round(mean("cost_pkr"), 2).alias("avg_cost_spent"),
    )

    appointment_data = df_treatment.groupby(
        "councillor_id", "patient_id", "category", "treatment"
    ).agg(
        count("*").alias("total_appointment"),
        max("treatment").alias("number_treatment"),
    )

    mean_appointment = appointment_data.groupBy("councillor_id").agg(
        round(mean("total_appointment"), 2).alias("avg_appointments"),
        count("number_treatment").alias("total_treatments"),
    )

    final_join = data_mean.join(mean_appointment, "councillor_id")

    return final_join


def transformation(data_frames):
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


def main():
    data_frames = load_data()
    outcome_data = transformation(data_frames)
    results_db(outcome_data)
    return outcome_data.show()


if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder.getOrCreate()
    main()


def schedule_job():
    schedule.every(1).minutes.do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)


# Schedule and run the job
schedule_job()
