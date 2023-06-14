from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, mean
from pyspark.sql.functions import *

import requests
import json

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Define a list of endpoints
end_points_list = ["appointment", "patient_councillor", "price_log", "councillor", "rating"]

# Create a dictionary to store the data frames
data_frames = {}

# Iterate over each endpoint
for end_point in end_points_list:
    variable_name = f"{end_point}_df"  # Generate a variable name for the data frame
    url = f"https://xloop-dummy.herokuapp.com/{end_point}"  # Construct the URL for the API request
    response = requests.get(url)  # Send a GET request to the API
    json_data = json.loads(response.text)  # Convert the JSON response to a Python dictionary
    data_frames[variable_name] = spark.createDataFrame(json_data)  # Create a Spark DataFrame from the JSON data

    
# Rename the "id" column to "councillor_id" in the "councillor_df" DataFrame
data_frames["councillor_df"] = data_frames["councillor_df"].withColumnRenamed("id", "councillor_id")

# Rename the "id" column to "patient_councillor_id" in the "patient_councillor_df" DataFrame
data_frames["patient_councillor_df"] = data_frames["patient_councillor_df"].withColumnRenamed("id", "patient_councillor_id")

# Rename the "id" column to "appointment_id" in the "appointment_df" DataFrame
data_frames["appointment_df"] = data_frames["appointment_df"].withColumnRenamed("id", "appointment_id")

# Rename the "value" column to "rating" in the "rating_df" DataFrame
data_frames["rating_df"] = data_frames["rating_df"].withColumnRenamed("value", "rating")


# Filter the "price_log_df" DataFrame to keep only rows where "is_active" is "true"
data_frames["price_log_df"] = data_frames["price_log_df"].filter(data_frames["price_log_df"]["is_active"] == "true")

# Filter the "appointment_df" DataFrame to keep only rows where "confirmed" is "true"
data_frames["appointment_df"] = data_frames["appointment_df"].filter(data_frames["appointment_df"]["confirmed"] == "true")


# Perform joins on the DataFrames to create a merged DataFrame
merged_df = data_frames["councillor_df"].join(data_frames["patient_councillor_df"], "councillor_id") \
    .join(data_frames["appointment_df"], "patient_id") \
    .join(data_frames["rating_df"], "appointment_id") \
    .join(data_frames["price_log_df"], "councillor_id")

# Select specific columns from the merged DataFrame
merged_df = merged_df.select("councillor_id", "patient_id", "rating", "amount_in_pkr")


#Removes rows with any missing values
cleaned_missing_values_df = merged_df.na.drop()

#Cleaning Duplicate Records
cleaned_df = cleaned_missing_values_df.dropDuplicates()


def SuccessRate(cleaned_df):
    
    # Create a new column "appointment_status" based on the "rating" column
    appointment_status_df = cleaned_df.withColumn("appointment_status", when(col("rating") >= 4, "successful").otherwise("unsuccessful"))


    # Group the appointment_status_df DataFrame by "councillor_id" and "patient_id"
    # Calculate the number of successful appointments and total appointments for each group
    # Calculate the success rate as a percentage
    success_rate_df = appointment_status_df.groupBy("councillor_id", "patient_id") \
        .agg(
            sum(when(col("appointment_status") == "successful", 1).otherwise(0)).alias("successful_appointment"),
            count("*").alias("total_appointment")
        ) \
        .withColumn("success_rate", round((col("successful_appointment") / col("total_appointment")) * 100, 2))

    # Group the success_rate_df DataFrame by "councillor_id"
    # Calculate the average success rate for each councillor
    avg_success_rate_df = success_rate_df.groupBy("councillor_id").agg(mean("success_rate").alias("success_rate"))
    
    return avg_success_rate_df

    
def Duration(cleaned_df):
    
    # Group the cleaned_df DataFrame by "councillor_id" and "patient_id"
    # Calculate the number of appointments for each group
    # Calculate the treatments duration by multiplying the number of appointments by 30
    treatments_duration_df = cleaned_df.groupBy("councillor_id", "patient_id") \
        .agg(count("*").alias("councillor_appointments")) \
        .withColumn("treatments_duration", col("councillor_appointments") * lit(30))

    # Group the treatments_duration_df DataFrame by "councillor_id"
    # Calculate the average total duration of appointments for each councillor
    avg_total_duration_of_appointments_df = treatments_duration_df.groupBy("councillor_id") \
        .agg(mean("treatments_duration").alias("avg_duration_per_treatment"))

    return avg_total_duration_of_appointments_df
    
    
def Cost(cleaned_df):    
    
    # Calculate the count of appointments by "councillor_id" for each "patient_id"
    concillor_appointments_df = cleaned_df.groupBy("councillor_id", "patient_id").agg(count("*").alias("concillor_appointments")) \
        .join(cleaned_df, "councillor_id")

    # Calculate the treatment cost by multiplying the "concillor_appointments" and "amount_in_pkr" columns
    treatment_cost_df = concillor_appointments_df.withColumn("treatment_cost", col("concillor_appointments") * col("amount_in_pkr"))

    # Calculate the average of the "treatment_cost" 
    avg_treatment_cost_df = treatment_cost_df.groupBy("councillor_id").agg(mean("treatment_cost").alias("avg_cost_per_treatment"))
    
    return avg_treatment_cost_df

    
def AppointmentsPerTreatment(cleaned_df):
    
    # Calculate the count of appointments by "councillor_id" for each "patient_id"
    concillor_appointments_df = cleaned_df.groupBy("councillor_id", "patient_id").agg(count("*").alias("concillor_appointments"))

    # Calculate the average no of appointments per treatment
    avg_concillor_appointments_df = concillor_appointments_df.groupBy("councillor_id") \
        .agg((round(mean("concillor_appointments"))).alias("avg_appointments_per_treatment"))
    
    return avg_concillor_appointments_df


success_rate_df = SuccessRate(cleaned_df)
duration_df = Duration(cleaned_df)
cost_df = Cost(cleaned_df)
appointments_df = AppointmentsPerTreatment(cleaned_df)


# Joining dataframes to create an outcomes table
Outcomes_table = success_rate_df.join(duration_df, "councillor_id") \
    .join(cost_df, "councillor_id") \
    .join(appointments_df, "councillor_id")

Outcomes_table.show()



# Write transformed data to a PostgreSQL database
Outcomes_table.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/outcome_prediction") \
    .option("dbtable", "result").option("user", "user").option("password", "password") \
    .option("driver", "org.postgresql.Driver").mode("overwrite").save()
