import pdb
import numpy
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, udf, col, lit
import pyspark.sql.functions as func
from pyspark.sql.types import Row, StructType,StructField,IntegerType,StringType,LongType,FloatType, ArrayType, DoubleType, DataType, MapType
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.mllib.linalg import Vectors, DenseVector
import time
import dateutil.parser as dparser
#import pandas
import datetime
import sys
import requests
from itertools import chain
import json
import gc


def script_step(step, name):
    print ("-------Step %s: %s-----------------"%(step,name))
    #requests.get('https://stubhub-collection-monitoring.mybluemix.net/spark-test?step=%s'%step)

def load_data(path, df_name):
	
	df = sqlCtx.read.format('json').load(path,  inferSchema="true").cache()
	count_before = float(df.count())
	print ("%s DF size before dropping nulls: %s" %(df_name,count_before))
	if '_corrupt_record ' in df.columns:
		df = df.filter("_corrupt_record is NULL")
		df = df.filter("_corrupt_record is null")

	df = df.na.drop(subset=["current_timestamp"])

	count_after = float(df.count())

	print ("count after: %s" %count_after)
	pct_drop=float(((count_before-count_after)/count_before)*100.0)


	print("%s Percentage dropped by nulls: %s %%" %(df_name,pct_drop))


	return df


def transform_inventory(inventory_df):

	
	inventory_df = inventory_df.select(inventory_df.current_timestamp,inventory_df.eventId, inventory_df.totalListings.alias("totalStadiumListings"), inventory_df.totalTickets.alias("totalStadiumTickets"), explode(inventory_df.section_stats).alias("section_stats_row"))
	inventory_df = inventory_df.select(inventory_df.current_timestamp.alias('timestamp'), inventory_df.eventId, inventory_df.section_stats_row.sectionId.alias('sectionId'), inventory_df.section_stats_row.averageTicketPrice.alias('averageTicketPrice'), inventory_df.section_stats_row.minTicketPrice.alias("minSectionTicketPrice"), inventory_df.section_stats_row.totalListings.alias('totalSectionListings'),inventory_df.section_stats_row.totalTickets.alias('totalSectionTickets'),inventory_df.totalStadiumListings, inventory_df.totalStadiumTickets ).alias('inventory_df2')

	print("Inventory df2 count: %s" %inventory_df.count())

	return inventory_df

def concat(team, timestamp):

	response = None

	try:
		date = datetime.datetime.utcfromtimestamp(int(str(timestamp)[0:10]))
		response= '%s#%s#%s#%s'  %(team, date.year, date.month, date.day)
	except:
		response = None

	return response

def transform_meta(meta_df):


	def get_home_name(performersCollection, performers):
		
		name = None
		team_id=None

		try:
			for performer in performersCollection:

				if 'role' not in performer or performer['role']=="HOME_TEAM":
					name = performer['name'].replace(" ","-")
					team_id = performer['id']
					break

			if name == None and len(performers)==1:
				name = performers[0]['name'].replace(" ","-")
				team_id = performers[0]['id']
		except:
			name = None
			team_id=None

		return {"name":name,"id":team_id}

	def get_away_name(performers, originalName):

		name = None
		team_id=None

		try:
			for performer in performers:

				if 'role' in performer and performer['role']=="AWAY_TEAM":
					name = performer['name'].replace(" ","-")
					team_id = performer['id']
					break

			if name == None:
				name = originalName.split(" at")[0].replace(" ", "-")
				if len(performers)>1:
					team_id = performers[1]['id']
		except:
			name = None
			team_id=None

		return {"name":name,"id":team_id}

	get_home_udf = udf(get_home_name, MapType(StringType(),StringType()))
	get_away_udf = udf(get_away_name, MapType(StringType(),StringType()))

	meta_df = meta_df.select(meta_df.current_timestamp.alias("timestamp"), meta_df.current_date, meta_df.events[0].id.alias('id'), meta_df.events[0].eventDateUTC.alias('event_date_utc'),meta_df.events[0].eventDateLocal.alias('event_date_local'), get_home_udf(meta_df.events[0].performersCollection, meta_df.events[0].performers)['id'].alias('Home_Team_Id'),get_home_udf(meta_df.events[0].performersCollection, meta_df.events[0].performers)['name'].alias("Home_Team_Name"),get_away_udf(meta_df.events[0].performersCollection,meta_df.events[0].originalName )['id'].alias('Away_Team_Id'),get_away_udf(meta_df.events[0].performersCollection, meta_df.events[0].originalName)['name'].alias('Away_Team_Name')).alias('meta_df').cache()


	try:
		print ("Away id nulls: %s" %meta_df.where(col("Away_Team_Id").isNull()).count())
		print ("Away name nulls: %s" %meta_df.where(col("Away_Team_Name").isNull()).count())
		print ("Home id nulls: %s" %meta_df.where(col("Home_Team_Id").isNull()).count())
		print ("Home name nulls: %s" %meta_df.where(col("Home_Team_Name").isNull()).count())
	except:
		print("failed counting nulls")

	meta_df = meta_df.na.drop(subset=['Away_Team_Id','Home_Team_Id'])

	meta_df = meta_df.groupBy(meta_df.id).agg({"timestamp": "max", "event_date_utc": "max", "event_date_local":"max","Home_Team_Id":"max", "Home_Team_Name":"max","Away_Team_Id":"max" }).select(meta_df.id, func.col("max(event_date_utc)").alias("event_date_utc"), func.col("max(event_date_local)").alias("event_date_local"), func.col("max(Home_Team_Id)").alias("Home_Team_Id"),func.col("max(Home_Team_Name)").alias("Home_Team_Name"), func.col("max(Away_Team_Id)").alias("Away_Team_Id"))

	# Add col for day of week
	get_weekday = udf(lambda x: dparser.parse(x).isoweekday())
	meta_df = meta_df.withColumn('DayofWeek', get_weekday(col("event_date_local")))

	# Add col for day, afternoon, night game
	get_game_time = udf(lambda x: 'd' if dparser.parse(x).hour<14 else ('a' if dparser.parse(x).hour<18 else 'n'))
	meta_df = meta_df.withColumn('GameTime', get_game_time(col("event_date_local")))
	

	print("Meta df count: %s" %meta_df.count())

	return meta_df


def transform_performance(performance_df):

	concat_udf = udf(concat, StringType())

	performance_df = performance_df.withColumn("Concatenated_perf", concat_udf(performance_df.team,performance_df.current_timestamp ))
	performance_df = performance_df.dropDuplicates(['Concatenated_perf'])

	return performance_df


def join_dfs(transformed_inventory, transformed_meta, transformed_performance):


	joined_df = transformed_inventory.join(transformed_meta,transformed_inventory.eventId == transformed_meta.id, 'inner').drop('id').alias('joined_df').cache()

	
	concat_udf = udf(concat, StringType())
	joined_df = joined_df.withColumn("Concatenated_joined", concat_udf(joined_df.Home_Team_Name,joined_df.timestamp )).cache()

	joined_df = joined_df.join(transformed_performance, joined_df.Concatenated_joined == transformed_performance.Concatenated_perf, 'left_outer').cache()

	count_before = float(joined_df.count())
	print(count_before)
	count_after = float(joined_df.where(col("Concatenated_perf").isNull()).count())
	print(count_after)
	print("Pct drop if removed nulls: %s %%"%(((count_before-count_after)/count_before)*100.0))

	print("Joined df2 count: %s" %count_before)
	
	return joined_df


def transform_days_diff(joined_df):

	def get_days_diff(timestamp, event_date):

	    event_date_timestamp = int(time.mktime(dparser.parse(event_date).timetuple())*1000)
	    ms_difference = float(event_date_timestamp - timestamp)
	    days_difference = ((((ms_difference)/1000)/60)/60)/24
	    response = float(days_difference)
	    return response

	udf_get_days_diff = udf(get_days_diff, DoubleType())

	transformed = joined_df.withColumn('Time_to_event', udf_get_days_diff(joined_df.timestamp, joined_df.event_date_utc)).cache()
	
	transformed = transformed.orderBy('Time_to_event', ascending=False).cache()
	transformed = transformed.orderBy('sectionId', ascending=False).cache()
	transformed = transformed.orderBy('eventId', ascending=False).cache()
	
	return transformed


def transform_categorical(df, categorical_columns, include_dense):

	# First consecutively index the column values
	indexers = [
	    StringIndexer(inputCol=c, outputCol="{0}_indexed".format(c), handleInvalid = "skip")
	    for c in categorical_columns
	]

	# Create a sparse vector of one hot encoding
	encoders = [OneHotEncoder(dropLast=False,inputCol=indexer.getOutputCol(),
	            outputCol="{0}_encoded".format(indexer.getOutputCol())) 
	    for indexer in indexers
	]

	# Assemble together all the categorical featurs sparse vectors into one big sparse vector
	assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders],outputCol="cat_features")

	print ("Assembling Sparse vectors for cat features...")
	# All in pipeline
	pipeline = Pipeline(stages=indexers + encoders+[assembler])
	model=pipeline.fit(df)
	transformed = model.transform(df).cache()

	# print("sparse vectors table...")
	# transformed.show()

	del df
	gc.collect()

	# Blow out features vector to multiple columns - one big dense vector
	def to_array(col):
	    def to_array_(v):
	        return v.toArray().tolist()
	    return udf(to_array_, ArrayType(DoubleType()))(col)


	vector_length = len(transformed.first().cat_features)

	if include_dense:
		transformed = (transformed.withColumn("cat_feature", to_array(col("cat_features")))
		.select([c for c in transformed.columns]+[col("cat_feature")[i] for i in range (vector_length)]))

	return transformed



if __name__ == "__main__":

	
	    
	setup_start = datetime.datetime.utcnow()

	path = sys.argv[1]

	inventory_paths = {"v3":"s3a://kartees-hadoop-data/event_inventory_hadoop_v3", "sample-set":"s3a://kartees-hadoop-data/inventory-sample-set" , "new-bucket":"s3a://kartees-hadoop-data/event_inventory_hadoop_v2","sample-small":"s3a://kartees-cloud-collection/spark-testing/event-inventory-small", "sample":"s3a://kartees-cloud-collection/spark-testing/event-inventory", "real":"s3a://kartees-cloud-collection/event_inventory"}
	meta_paths = {"v3":"s3a://kartees-hadoop-data/event_metadata_v2_hadoop_v3", "sample-set":"s3a://kartees-hadoop-data/meta-sample-set" ,"new-bucket":"s3a://kartees-hadoop-data/event_metadata_v2_hadoop","sample-small":"s3a://kartees-cloud-collection/spark-testing/event-metadata-small","sample":"s3a://kartees-cloud-collection/spark-testing/event-metadata", "real":"s3a://kartees-cloud-collection/event_metadata_v2"}
	performance_paths = {"v3":"s3a://kartees-hadoop-data/performance_hadoop_v3", "sample-set":"s3a://kartees-hadoop-data/performance_sample_set"}

	inventory_path = inventory_paths[path]
	meta_path = meta_paths[path]
	performance_path = performance_paths[path]

	conf = SparkConf().setAppName("test kartees structuring")
	sc = SparkContext(conf=conf)
	sqlCtx = SQLContext(sc)
	log4j = sc._jvm.org.apache.log4j
	log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)


	execution_start = datetime.datetime.utcnow()

#-------------------------------------------------------------------------------
#------------------Load / Transform Inventory Data------------------------------
#-------------------------------------------------------------------------------
	
	script_step(1, 'Loading Event Inventory data')

	inventory_df = load_data(inventory_path, 'Inventory').cache()
	
	script_step(2, 'Transforming inventory data - exploding by sections')

	transformed_inventory = transform_inventory(inventory_df).cache()

	
	del inventory_df
	gc.collect()
	#inventory_df.unpersist()
#-------------------------------------------------------------------------------
#------------------Load / Transform Event Meta Data-----------------------------
#-------------------------------------------------------------------------------

	script_step(3, 'Loading Event Metadata')

	meta_df = load_data(meta_path, 'Meta').cache()

	script_step(4, 'Transforming Event Metadata - finding home, away teams')

	transformed_meta = transform_meta(meta_df).cache()

	del meta_df
	gc.collect()
	#meta_df.unpersist()
#-------------------------------------------------------------------------------
#------------------Load / Transform Performance Data-----------------------------
#-------------------------------------------------------------------------------

	script_step(5, 'Loading Team Performance data')

	performance_df = load_data(performance_path, 'Performance').cache()

	script_step(6, 'Transforming Performance data')

	transformed_performance = transform_performance(performance_df).cache()

	del performance_df
	gc.collect()
	#performance_df.unpersist()
#-------------------------------------------------------------------------------
#------------------Join all 3 - inventory, meta, performance--------------------
#-------------------------------------------------------------------------------

	script_step(7, 'Joining all 3 dataframes')

	joined_df = join_dfs(transformed_inventory,transformed_meta, transformed_performance).cache()

	del transformed_meta
	del transformed_inventory
	gc.collect()
	#transformed_meta.unpersist()
	#transformed_inventory.unpersist()
#-------------------------------------------------------------------------------
#------------------Add Days diff Col -------------------------------------------
#-------------------------------------------------------------------------------

	script_step(8, 'Transforming - calculating time before event column')

	transformed_days_diff = transform_days_diff(joined_df).cache()

	del joined_df
	gc.collect()
	#joined_df.unpersist()
#-------------------------------------------------------------------------------
#---------------Transform categorical cols to One Hot Encoded vectors ----------
#-------------------------------------------------------------------------------
	
	script_step(9, 'Adding Sparse vectors for one hot encodings of categorical features')

	categorical_columns = ['Home_Team_Id', 'Away_Team_Id', 'sectionId', 'DayofWeek','GameTime']

	transformed_categorical = transform_categorical(transformed_days_diff, categorical_columns, include_dense=False).cache()

	del transformed_days_diff
	gc.collect()
	#transformed_days_diff.unpersist()

	print("Final count: %s" %transformed_categorical.count())

	print ('DONE!!')

	all_time_elapsed = "%.2f" %float(float((datetime.datetime.utcnow() - setup_start).seconds))
	execution_elapsed= "%.2f" %float(float((datetime.datetime.utcnow() - execution_start).seconds))


	print("--------------------Total (setup+execution) Seconds elapsed: %s---------" %all_time_elapsed)
	print("--------------------Execution Seconds elapsed: %s-----------------------" %execution_elapsed)

#-------------------------------------------------------------------------------
#---------------Output to Parquet and S3 ---------------------------------------
#-------------------------------------------------------------------------------
	
	script_step(10, 'Writing file to Parquet format on S3')

	t = datetime.datetime.utcnow()
	file = ("s3a://kartees-ai/all_structured/%s_%s.parquet" %(path,t.strftime( "%Y_%m_%d_%H_%M_%S")))
	print("Saving at %s" %file)
	transformed_categorical.write.format("parquet").save(file)

	df = transformed_categorical

	del transformed_categorical
	gc.collect()
	transformed_categorical.unpersist()
#-------------------------------------------------------------------------------
#---------------Output to Multiple structured CSVs to S3 -----------------------
#-------------------------------------------------------------------------------
	
	script_step(11, 'Splitting to multiple DFs for each eventId')

	# Get unique list of event Ids
	listids = [x.asDict().values()[0] for x in df.select("eventId").distinct().collect()]

	# Create array of DFs
	dfArray = [df.where(df.eventId == x) for x in listids]

	dfArray[0].show()
	print("First array size: %s" %dfArray[0].count())

	dfArray[1].show()

	del df 
	gc.collect()