import pymongo
from pyspark import SparkContext, SparkConf
import pymongo_spark
import findspark
findspark.init()
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
pymongo_spark.activate()

conf = SparkConf().setAppName("mongo-hadoop")
sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("Operations").getOrCreate()

G= sqlContext.read.format('csv').options(header='true').load('/d_pretraité/part-00000-41ded1db-06f0-498a-a393-9389df0061d5-c000.csv')
#G=sc.text
GA=G.rdd.map(lambda row: row.asDict())
GA.saveToMongoDB('mongodb://localhost:27017/POC1.données')
