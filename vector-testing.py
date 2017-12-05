import pdb
import numpy
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import explode, udf, col, lit
import pyspark.sql.functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,LongType,FloatType, ArrayType, DoubleType
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
import time
import dateutil.parser as dparser
#import pandas
import datetime
import sys
import requests
from itertools import chain
from pyspark.ml.linalg import Vectors

conf = SparkConf().setAppName("test kartees structuring")
sc = SparkContext(conf=conf)
sqlCtx = SQLContext(sc)
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)


v1 = Vectors.dense([1,3,4,2.5])
v2 = Vectors.dense([1,3,6])
l = [('Alice', 1), ('BOB',2)]

df = sqlCtx.createDataFrame(l)

df.show()

df.printSchema()

t = df.rdd

print(t.take(2))