import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql import types
from  pyspark.sql.functions import input_file_name
import pandas as pd
from  pyspark.sql.functions import input_file_name, col, substring, regexp_replace, udf
import pyspark.sql.functions as f
# parser = argparse.ArgumentParser()

# parser.add_argument('--input_files', required=True)
# parser.add_argument('--output', required=True)

# args = parser.parse_args()
# input_files = args.input_files
# output = args.output

spark = SparkSession.builder \
    .appName('ukraine-tweets') \
    .getOrCreate()
bucket = "dtc_data_lake_ukraine-tweets-381418"
spark.conf.set('temporaryGcsBucket', bucket)
def spark_job(data_folder: str, mode: str):
    schema = types.StructType([
                    types.StructField('_c0', types.LongType(), True),
                    types.StructField('userid', types.LongType(), True),
                    types.StructField('username', types.StringType(), True),
                    types.StructField('acctdesc', types.StringType(), True),
                    types.StructField('location', types.StringType(), True),
                    types.StructField('following', types.LongType(), True),
                    types.StructField('followers', types.LongType(), True),
                    types.StructField('totaltweets', types.LongType(), True),
                    types.StructField('usercreatedts', types.TimestampType(), True),
                    types.StructField('tweetid', types.LongType(), True),
                    types.StructField('tweetcreatedts', types.TimestampType(), True),
                    types.StructField('retweetcount', types.IntegerType(), True),
                    types.StructField('text', types.StringType(), True),
                    types.StructField('hashtags', types.StringType(), True),
                    types.StructField('language', types.StringType(), True),
                    types.StructField('coordinates', types.DoubleType(), True),
                    types.StructField('favorite_count', types.LongType(), True),
                    types.StructField('extractedts', types.TimestampType(), True),
    ])
    df_files = spark \
        .read \
        .format("csv") \
        .option("delimiter", ",").option("header", "true").option("multiLine", 'true').option("lineSep", '\n').option("quote", "\"") \
        .option("escape", "\"")\
        .schema(schema) \
        .csv(f"gs://dtc_data_lake_ukraine-tweets-381418/{data_folder}/")
    df_files = df_files.drop("_c0") \
        .withColumnRenamed("acctdesc","account_description") \
        .withColumnRenamed("usercreatedts","user_creation_date") \
        .withColumnRenamed("tweetcreatedts","tweet_creation_date") \
        .withColumnRenamed("usercreatedts","user_creation_date") \
        .withColumn("filename", input_file_name())
    df_files = df_files.withColumn('date', substring('tweet_creation_date', 0,10))
    df_files = df_files.withColumn('cleaned_text', regexp_replace("text", "[^0-9a-zA-Z_\-]+", " "))
    df_files = df_files.withColumn("text_cleaned",f.lower(f.col("cleaned_text")))
    df_files = df_files.drop("text", "cleaned_text") 
    df_files.write.format('bigquery') \
        .option('table', 'ukraine_tweets_all.ukraine-tweets-cleaned') \
        .mode(f'{mode}') \
        .save()
    
if __name__ == "__main__":
    df_intial = pd.read_csv('gs://dtc_data_lake_ukraine-tweets-381418/code/load.csv')
    if df_intial.loc[:, 'check'].item() ==0:
        print("This is the task for inital load that will run once")
        spark_job("data", "overwrite")
        print('now we will change the value of the storage in the csv file')
        df_intial.loc[:, 'check'] = 1
        df_intial.to_csv('gs://dtc_data_lake_ukraine-tweets-381418/code/load.csv', index=False)
    else:
         spark_job("delta", "append")
