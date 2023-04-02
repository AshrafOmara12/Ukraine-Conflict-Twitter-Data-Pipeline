import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql import types
from  pyspark.sql.functions import input_file_name
import pandas as pd

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
# spark.conf.set('temporaryGcsBucket', 'dataproc-staging-europe-north1-870836830798-dhu8a1tw')
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

df_intial = pd.read_csv('gs://dtc_data_lake_ukraine-tweets-381418/code/load.csv')
if df_intial.loc[:, 'check'].item() ==0:
    print("This is the task for inital load that will run once")
    df_files = spark \
        .read \
        .format("csv") \
        .option("delimiter", ",").option("header", "true").option("multiLine", 'true').option("lineSep", '\n').option("quote", "\"") \
        .option("escape", "\"")\
        .schema(schema) \
        .csv("gs://dtc_data_lake_ukraine-tweets-381418/data/")
    df_files = df_files.drop("_c0") \
        .withColumnRenamed("acctdesc","account_description") \
        .withColumnRenamed("usercreatedts","user_creation_date") \
        .withColumnRenamed("tweetcreatedts","tweet_creation_date") \
        .withColumnRenamed("usercreatedts","user_creation_date") \
        .withColumn("filename", input_file_name())

    df_files.write.format('bigquery') \
        .option('table', 'ukraine_tweets_all.ukraine-tweets') \
        .mode('overwrite') \
        .save()
    print('now we will change the value of the storage in the csv file')
    df_intial.loc[:, 'check'] = 1
    df_intial.to_csv('gs://dtc_data_lake_ukraine-tweets-381418/code/load.csv', index=False)
else:
    print(' this the the delta load')
    df_detla = spark \
        .read \
        .format("csv") \
        .option("delimiter", ",").option("header", "true").option("multiLine", 'true').option("lineSep", '\n').option("quote", "\"") \
        .option("escape", "\"")\
        .schema(schema) \
        .csv("gs://dtc_data_lake_ukraine-tweets-381418/delta/")

    df_detla = df_detla.drop("_c0") \
        .withColumnRenamed("acctdesc","account_description") \
        .withColumnRenamed("usercreatedts","user_creation_date") \
        .withColumnRenamed("tweetcreatedts","tweet_creation_date") \
        .withColumnRenamed("usercreatedts","user_creation_date") \
        .withColumn("filename", input_file_name())

    df_detla.write.format('bigquery') \
        .option('table', 'ukraine_tweets_all.ukraine-tweets') \
        .mode('append') \
        .save()
