from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, avg, datediff, month, max, col, count, row_number
from pathlib import Path
from datetime import timedelta, datetime
from pyspark.sql.window import Window
import os

"""
A few notes for this script:
1) PySpark needs Java 17/21 (17 works better) to work; it will error with a newer version like Java 24
2) Spark can't read a .zip (it can read other compressed formats like .gz)
So for simplicity, I just unzipped the files to keep the focus on one task; converting zip formats
could be added, but for this script using assumption that input is files in an ingestable format
"""

input_files = {
    'Exercises/Exercise-6/data/Divvy_Trips_2019_Q4/Divvy_Trips_2019_Q4.csv'
}



def get_average_durations(df,group_by_column):
    print(f"Calculating average duration based on {group_by_column}.")
    avg_duration_df = df.groupBy(col(group_by_column)).agg(avg("duration").alias("average"))
    return avg_duration_df


def get_trips_per_day(df):
    print(f"Calculating trips per day.")
    trips_per_day_df = df.groupby("date").count()
    return trips_per_day_df


def get_top_starting_location_per_month(df):
    print(f"Calculating top starting locations per month.")
    df_with_month = df.withColumn("month", month(df["date"]))
    top_starting_locations_df = df_with_month.groupBy("month","from_station_name").count().orderBy("count", ascending=False)
    return top_starting_locations_df


def limit_df_to_n_days(df,days):
    print(f"Reducing data frame to last {days} day(s) to allow additional calculations.")
    max_date = df.agg(max("date").alias("max_date")).first()["max_date"]
    start_date = max_date - timedelta(days=int(days))

    filtered_df = df.filter(col("date") >= start_date.strftime("%Y-%m-%d"))
    return filtered_df


def get_top_n_starting_location_per_day(df, limit):
    print(f"Getting the top {limit} starting location(s) per day.")

    #Get initial counts per day
    daily_counts = df.groupBy("date", "from_station_name").agg(count("*").alias("count"))

    # Create windows function specification allow rank application
    window_spec = Window.partitionBy("date").orderBy(col("count").desc())

    top_stations = daily_counts.withColumn("rank", row_number().over(window_spec)) \
                        .filter(col("rank") <= limit)

    return top_stations


def get_top_n_ages_per_trips(df,limit):
    print(f"Getting the top and bottom {limit} ages per trip duration.")

    window_spec_desc = Window.partitionBy("age").orderBy(col("duration").desc())
    window_spec_asc = Window.partitionBy("age").orderBy(col("duration").asc())

    df_with_ranks = df \
                .withColumn("top_rank", row_number().over(window_spec_desc)) \
                .withColumn("bottom_rank", row_number().over(window_spec_asc))

    top_df = df_with_ranks.filter(col("top_rank") <= limit)
    bottom_df = df_with_ranks.filter(col("bottom_rank") <= limit)

    combined_df = top_df.union(bottom_df)
    return combined_df



def read_csv_with_spark(spark, csv_path):
    print(f"Reading in csv: {csv_path}")

    df = spark.read.option("header", "true").csv(csv_path)
    current_year = datetime.now().year

    df_with_date = df.withColumn("date", to_date("start_time")) \
                        .withColumn("duration",datediff("end_time","start_time")*24*60) \
                        .withColumn("age",current_year - col("birthyear").cast("int"))
    return df_with_date


def write_df_to_csv(df, output_path):

    ## Output function to write to csv
    ## Spark does not write to a single csv, but multiple pieced files
    ## Spark also requires configuring Hadoop first to use for output
    ## If Hadoop is working, then should output file to directory

    os.makedirs(output_path,exist_ok=True)
    
    df.coalesce(1) \
      .write \
      .mode("overwrite") \
      .option("header", True) \
      .csv(output_path)


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    
    for file in input_files:
        ## Spark had issues with relative path, 
        ## so find absolute path and use it
        absolute_path = str(Path(file).resolve())
        df_with_date = read_csv_with_spark(spark, absolute_path)

        get_average_durations(df_with_date,'date').show()
        get_trips_per_day(df_with_date).show()
        get_top_starting_location_per_month(df_with_date).show()

        two_week_df = limit_df_to_n_days(df_with_date,14)
        get_top_n_starting_location_per_day(two_week_df,3).show()
        get_average_durations(df_with_date,'gender').show()
        get_top_n_ages_per_trips(df_with_date,10).show()



if __name__ == "__main__":
    main()


