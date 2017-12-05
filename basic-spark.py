from pyspark import SparkContext

sc = SparkContext()
firstrdd = sc.parallelize([1,2,3])

print ("-------------------Count is: %s----------------------------" %firstrdd.count())






