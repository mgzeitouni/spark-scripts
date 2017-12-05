
import pdb
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode
import pyspark.sql.functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,LongType,FloatType
import time
import dateutil.parser as dparser
#import pandas
import datetime
import sys
import requests


def call_API(step):
    print ("----------------Step %s-----------------"%step)
    requests.get('https://stubhub-collection-monitoring.mybluemix.net/spark-test?step=%s'%step)
    
#call_API(1)
    

start = datetime.datetime.utcnow()

conf = SparkConf().setAppName("test kartees structuring")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)


path="s3a://kartees-ai/price-through-time/new-bucket_2017_11_09_18_36_35.parquet"

print("Reading parquet directory...")
df = sqlCtx.read.parquet(path)
print("Regular df count: %s"%df.count())
df1 = df.dropna()
print("dropped null df count: %s"%df1.count())
# call_API(2)


# t = datetime.datetime.utcnow()

# file = ("s3a://kartees-ai/price-through-time_%s.csv" %t.strftime( "%Y_%m_%d_%H_%M_%S"))
# print("Saving at %s" %file)

# df.write.csv(file)
# execution_elapsed= "%.2f" %float(float((datetime.datetime.utcnow() - start).seconds))


# print("--------------------Execution Seconds elapsed: %s-----------------------" %execution_elapsed)


# call_API(3)