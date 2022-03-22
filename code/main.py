import os
from pyspark.sql.session import SparkSession
import schema
from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType, TimestampType, IntegerType, FloatType
from pyspark.sql import column as col
from pyspark.sql.window import Window
from pyspark.sql import Row
from itertools import islice
from datetime import datetime

# Load CSV file using the custom schema, drop the malformed records
# Since we have 5 different structures in the dataset, its required to skip the malformed records
# Since the format difference is noticed in 2021, the last 14 fourteen days does not have nay schema difference
def read_csv_data(spark, path, schema):
    return spark.read.schema(schema) \
        .option("header","true") \
        .option("mode","DROPMALFORMED") \
        .csv(path)

if __name__ == '__main__':
    # Load Spark master information from Environment Variable
    # Initialize SparkSession
    spark = SparkSession.builder \
        .master(os.environ.get("SPARK_MASTER","spark://192.168.1.1:7077")) \
        .appName("Covid-19 Analysis") \
        .getOrCreate()

    #Load Schema
    #FIPS,Admin2,Province_State,Country_Region,Last_Update,Lat,Long_,Confirmed,Deaths,Recovered,Active,Combined_Key,Incident_Rate,Case_Fatality_Ratio
    covid_19_schema_final = schema.covid_19_schema_final

    #Set Input Path
    #This can also be read from config file or as environment variable
    source_path = "/home/ec2-user/COVID-19/csse_covid_19_data/csse_covid_19_daily_reports/*.csv"

    # Load only last 14 days data
    source_data_frame = read_csv_data(spark, source_path, covid_19_schema_final) \
            .filter(date_sub(current_date(),14) <= to_date("Last_Update") )

    source_data_frame.cache()
    source_data_frame.printSchema()

    # Finding Decreasing trend
    decreasing_positives_prep = source_data_frame \
        .select("Country_Region",to_date("Last_Update").alias("Last_Update"),"Confirmed") \
        .groupBy("Country_Region","Last_Update").agg(sum(expr("Confirmed")).alias("Positive_cases_by_country")) \
        .withColumn("Lead_cases_by_country",lag("Positive_cases_by_country",1,"Positive_cases_by_country").over(Window.partitionBy("Country_Region").orderBy("Last_Update"))) \
        .withColumn("Case_difference", expr("Positive_cases_by_country -  Lead_cases_by_country")) \
        .withColumn("Trend", when(expr("Case_difference is null or Case_difference == 0"),'NO_CHANGE') \
            .when(expr("Case_difference < 0"), 'DECREASED').otherwise('INCREASED')) 
    
    #Stage data to debug
    #decreasing_positives_prep.write.option("header","true").csv("prep_data")

    # Generate Top 10 country data
    top10_countries_decreasing_positives = decreasing_positives_prep \
        .filter("Trend == 'DECREASED'") \
        .withColumn("top10_rank",dense_rank().over(Window.orderBy(desc("Positive_cases_by_country")))) \
        .filter("top10_rank <= 10") \
        .select("Country_Region","Last_Update","Positive_cases_by_country") \
        .orderBy(desc("Positive_cases_by_country")) 


    top10_countries = top10_countries_decreasing_positives.select(expr("Country_Region"),expr("Last_Update")).distinct()

    # Generate Top 3 states for the top countries
    top3_states_high_positives = source_data_frame.select("Country_Region","Province_State", \
                to_date("Last_Update").alias("Last_Update"),expr("Confirmed").alias("Positive_cases_by_state")) \
            .join(top10_countries, ["Country_Region","Last_Update"], "inner") \
            .select("Country_Region","Last_Update","Province_State","Positive_cases_by_state") \
            .withColumn("TOP_RANK", dense_rank().over(Window.partitionBy("Country_Region") \
                .orderBy(desc("Positive_cases_by_state")))) \
            .filter("TOP_RANK <= 3") \
            .select("Country_Region","Province_State","Last_Update","Positive_cases_by_state") \
            .orderBy(desc("Positive_cases_by_state"))

    top10_countries_decreasing_positives.write.option("header","true").csv("top10_countries_decreasing_cases")

    top3_states_high_positives.write.option("header","true").csv("top3_states_high_positives")

    # Close/Clean the spark
    spark.stop()
