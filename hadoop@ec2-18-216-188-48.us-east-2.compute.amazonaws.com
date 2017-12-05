
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
    
call_API(1)
    
setup_start = datetime.datetime.utcnow()

#path = sys.argv[1]
path="sample-set"
inventory_paths = {"sample-set":"s3a://kartees-hadoop-data/inventory-sample-set" , "new-bucket":"s3a://kartees-hadoop-data/event_inventory_hadoop","sample-small":"s3a://kartees-cloud-collection/spark-testing/event-inventory-small", "sample":"s3a://kartees-cloud-collection/spark-testing/event-inventory", "real":"s3a://kartees-cloud-collection/event_inventory"}
meta_paths = {"sample-set":"s3a://kartees-hadoop-data/meta-sample-set" ,"new-bucket":"s3a://kartees-hadoop-data/event_metadata_v2_hadoop","sample-small":"s3a://kartees-cloud-collection/spark-testing/event-metadata-small","sample":"s3a://kartees-cloud-collection/spark-testing/event-metadata", "real":"s3a://kartees-cloud-collection/event_metadata_v2"}

inventory_path = inventory_paths[path]
meta_path = meta_paths[path]

#inventory_path = 's3a://kartees-cloud-collection/event_inventory'
#meta_path = 's3a://kartees-cloud-collection/event_metadata'

conf = SparkConf().setAppName("test kartees structuring")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)


execution_start = datetime.datetime.utcnow()

#conf.set("fs.s3a.access.key",aws_key )  
#conf.set("fs.s3a.secret.key", aws_secret)
inventory_df = sqlCtx.read.format('json').load(inventory_path)

#print ("DF size before dropping nulls: %s" %inventory_df.count())
# inventory_df.dropna()
# print("DF size after dropping nulls: %s" %inventory_df.count())
call_API(2)
#inventory_df.printSchema()
inventory_df.show(5)

# data_rdd = dataframe.rdd
inventory_df1 = inventory_df.select(inventory_df.current_timestamp,inventory_df.eventId, explode(inventory_df.section_stats).alias("section_stats_row"))
inventory_df2 = inventory_df1.select(inventory_df1.current_timestamp.alias('timestamp'), inventory_df1.eventId, inventory_df1.section_stats_row.sectionId.alias('sectionId'), inventory_df1.section_stats_row.averageTicketPrice.alias('averageTicketPrice')).alias('inventory_df2')
inventory_df2.show(5)

#print(df2.collect()[0])
call_API(3)
meta_df = sqlCtx.read.format('json').load(meta_path)
# meta_df.printSchema()
meta_df1 = meta_df.select(meta_df.current_timestamp.alias("timestamp"), meta_df.events[0].groupingsCollection[0].id.alias('id'), meta_df.events[0].eventDateUTC.alias('event_date')).alias('meta_df1')
meta_df2 = meta_df1.groupBy(meta_df1.id).agg({"timestamp": "max", "event_date": "max"}).select(meta_df1.id, func.col("max(event_date)").alias("event_date"))
meta_df2.show()

joined_df = inventory_df2.join(meta_df2,inventory_df2.eventId == meta_df2.id, 'left_outer').drop('id').alias('joined_df')
call_API(4)

def transform(timestamp, event_date):
	try:
	    event_date_timestamp = int(time.mktime(dparser.parse(event_date).timetuple())*1000)
	    ms_difference = float(event_date_timestamp - timestamp)
	    days_difference = ((((ms_difference)/1000)/60)/60)/24
	    response = float(days_difference)
	except:
		response = None

	return response

final_df = joined_df.rdd.map(lambda x: [ transform(x.timestamp, x.event_date),x.sectionId,x.averageTicketPrice])\
                .toDF(StructType([StructField('days_difference', FloatType()),StructField('sectionId', IntegerType()),StructField('averageTicketPrice', FloatType())]))
                
final_df.show()

call_API(5)

all_time_elapsed = "%.2f" %float(float((datetime.datetime.utcnow() - setup_start).seconds))
execution_elapsed= "%.2f" %float(float((datetime.datetime.utcnow() - execution_start).seconds))

print("--------------------Total (setup+execution) Seconds elapsed: %s---------" %all_time_elapsed)
print("--------------------Execution Seconds elapsed: %s-----------------------" %execution_elapsed)
call_API(6)
t = datetime.datetime.utcnow()
file = ("s3a://kartees-ai/price-through-time/%s_%s.parquet" %(path,t.strftime( "%Y_%m_%d_%H_%M_%S")))
print("Saving at %s" %file)

final_df.write.format("parquet").save(file)
call_API(7)