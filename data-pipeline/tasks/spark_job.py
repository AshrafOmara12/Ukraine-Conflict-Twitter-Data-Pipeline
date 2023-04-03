import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql import types
from  pyspark.sql.functions import input_file_name, when, col, substring, regexp_replace, udf
import pandas as pd
import pyspark.sql.functions as f
from textblob import TextBlob
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem import SnowballStemmer
import sys

nltk.download('vader_lexicon')

initial = sys.argv[1]
output_path = sys.argv[2]
# bucket_name = sys.argv[6]
# delta = sys.argv[9]


spark = SparkSession.builder \
    .appName('ukraine-tweets') \
    .getOrCreate()
bucket = "dtc_data_lake_ukraine-tweets-381418"
spark.conf.set('temporaryGcsBucket', bucket)

def get_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity
def remove_rt(x): return re.sub('RT @\w+: ', " ", x)
 
def rt(x): return re.sub(
    "(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", x)

def spark_job(data_folder: str, mode: str, output: str):
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
        .csv(data_folder)
    df_files = df_files.drop("_c0") \
        .withColumnRenamed("acctdesc","account_description") \
        .withColumnRenamed("usercreatedts","user_creation_date") \
        .withColumnRenamed("tweetcreatedts","tweet_creation_date") \
        .withColumnRenamed("usercreatedts","user_creation_date") \
        .withColumn("filename", input_file_name())
    remove_rt_udf = udf(lambda z: remove_rt(z))
    rt_udf = udf(lambda z: rt(z))
    df_files = df_files.withColumn('date', substring('tweet_creation_date', 0,10))
    df_files = df_files.withColumn('cleaned_text', remove_rt_udf("text"))
    df_files = df_files.withColumn('text_cleaned', rt_udf("text"))
    df_files = df_files.withColumn("text_cleaned",f.lower(f.col("text_cleaned")))
    df_files = df_files.drop("text", "cleaned_text") 
    sentiment_udf = udf(get_sentiment, types.FloatType())
    df_files = df_files.withColumn("sentiment", sentiment_udf("text_cleaned"))
    df_files = df_files.withColumn('sentiment_analysis', when(df_files['sentiment'] > 0, "positive") \
                                .when(df_files['sentiment'] < 0, "negative").otherwise("neutral"))

    df_files.write.format('bigquery') \
        .option('table', output) \
        .mode(f'{mode}') \
        .partitionBy("tweetcreatedts", "language") \
        .save()
    
if __name__ == "__main__":
    df_intial = pd.read_csv('gs://dtc_data_lake_ukraine-tweets-381418/code/code_load.csv')
    if df_intial.loc[:, 'check'].item() == 0:
        print("This is the task for inital load that will run once")
        spark_job(initial, "overwrite", output_path)
        print('now we will change the value of the storage in the csv file')
        df_intial.loc[:, 'check'] = 1
        df_intial.to_csv('gs://dtc_data_lake_ukraine-tweets-381418/code/code_load.csv', index=False)
    else:
         spark_job("gs://dtc_data_lake_ukraine-tweets-381418/delta/", "append")
