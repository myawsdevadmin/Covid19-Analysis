from pyspark.sql.types import StructField, StructType, StringType, TimestampType, IntegerType, FloatType

covid_19_schema_final = StructType([ StructField("FIPS",StringType(),True),
                 StructField("Admin2",StringType(),True),
                 StructField("Province_State",StringType(),True),
                 StructField("Country_Region",StringType(),True),
                 StructField("Last_Update",TimestampType(),True),
                 StructField("Lat",FloatType(),True),
                 StructField("Long_",FloatType(),True),
                 StructField("Confirmed",IntegerType(),True),
                 StructField("Deaths",IntegerType(),True),
                 StructField("Recovered",IntegerType(),True),
                 StructField("Active",IntegerType(),True),
                 StructField("Combined_Key",StringType(),True),
                 StructField("Incident_Rate",FloatType(),True),                 
                 StructField("Case_Fatality_Ratio",FloatType(),True),
                 #StructField("_corrupt_record",StringType(),True)
                ])                
