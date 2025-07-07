from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, avg, datediff, month, max, col, count, row_number
from pathlib import Path
from datetime import timedelta
from pyspark.sql.window import Window

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
    daily_avg_duration_df = df.groupBy(f"{group_by_column}").agg(avg(datediff("end_time","start_time")*24*60).alias("average"))
    return daily_avg_duration_df


def get_trips_per_day(df):
    trips_per_day_df = df.groupby("date").count()
    
    return trips_per_day_df


def get_top_starting_location_per_month(df):
    df_with_month = df.withColumn("month", month(df["date"]))
    top_starting_locations_df = df_with_month.groupBy("month","from_station_name").count().orderBy("count", ascending=False)
    return top_starting_locations_df


def limit_df_to_two_weeks(df):
    max_date = df.agg(max("date").alias("max_date")).collect()[0]["max_date"]
    two_weeks_ago = max_date - timedelta(days=14)

    filtered_df = df.filter(col("date") >= two_weeks_ago.strftime("%Y-%m-%d"))
    return filtered_df


def get_top_3_starting_location_per_day(df):
    
    #Get initial counts per day
    daily_counts = df.groupBy("date", "from_station_name").agg(count("*").alias("count"))

    # Create windows function specification allow rank application
    window_spec = Window.partitionBy("date").orderBy(col("count").desc())

    top_stations = daily_counts.withColumn("rank", row_number().over(window_spec)) \
                        .filter(col("rank") <= 3)

    return top_stations


def read_csv_with_spark(spark, csv_path):
    df = spark.read.option("header", "true").csv(csv_path)
    df_with_date = df.withColumn("date", to_date("start_time"))
    return df_with_date


def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    
    for file in input_files:
        ## Spark had issues with relative path, 
        ## so find absolute path and use it
        absolute_path = str(Path(file).resolve())
        df_with_date = read_csv_with_spark(spark, absolute_path)

        get_average_durations(df_with_date,'date').show()
        # get_trips_per_day(df_with_date).show()
        # get_top_starting_location_per_month(df_with_date).show()

        #two_week_df = limit_df_to_two_weeks(df_with_date)
        #get_top_3_starting_location_per_day(two_week_df).show()
        get_average_durations(df_with_date,'gender').show()



if __name__ == "__main__":
    main()


