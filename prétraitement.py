import findspark
findspark.init()
import pyspark
import random
from pyspark import SparkFiles
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkConf
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import subprocess
import pandas as pd
import pydoop
import pydoop.hdfs as hdfs
from hdfs.client import InsecureClient
import os
from pyspark.sql.types import *

#os.environ["HADOOP_USER_NAME"] ="hdfs"
os.environ["PYTHON_VERSION"] = "3.7.3"

spark = SparkSession.builder.appName('POC_Pyspark').getOrCreate()
from pyspark.ml.clustering import KMeans
taxiFares = spark.read.csv("/projet/taxiFares",inferSchema=True ,sep =',')
taxiride=spark.read.csv("/projet/taxiride",inferSchema=True,sep =',')

taxiFares= taxiFares.selectExpr("cast(_c0 as Long) rideId", "cast(_c1 as Long) taxiId", "cast(_c2 as Long) driverId",
"cast(_c3 as Timestamp) startTime", "cast(_c4 as String) paymentType",
"cast(_c5 as Float) tip", "cast(_c6 as Float) tolls",
                                "cast(_c7 as Float) totalFare")
taxiride= taxiride.selectExpr("cast(_c0 as Long) rideId", "cast(_c1 as String) isStart", "cast(_c2 as Timestamp) endTime",
"cast(_c3 as Timestamp) startTime", "cast(_c4 as Float) startLon","cast(_c5 as Float) startLat", "cast(_c6 as Float) endLon",
"cast(_c7 as Float) endLat", "cast(_c8 as Short) passengerCnt","cast(_c9 as Long) taxiId", "cast(_c10 as Float) driverId")



Fares = taxiFares \
  .selectExpr("rideId AS rideId_fares", "startTime", "totalFare", "tip")
Rides = taxiride \
.selectExpr("rideId", "endTime", "driverId", "taxiId", \
  "startLon", "startLat", "endLon", "endLat") 
lonEast = -73.887
lonWest = -74.037
latNorth = 40.899
latSouth = 40.695

taxi = Fares.join(Rides, (Fares.rideId_fares == Rides.rideId) & (Rides.endTime > Fares.startTime) , how='inner')
taxi = taxi.drop(taxi["rideId_fares"]).filter((col("tip") > 0) & (col("startLon") >= lonWest) & (col("startLon") <= lonEast) & (col("startLon") >= lonWest) & (col("startLon") <= lonEast) & (col("startLat") >= latSouth) & (col("startLat") <= latNorth) &
        (col("endLon") >= lonWest) & (col("endLon") <= lonEast) &
        (col("endLat") >= latSouth) & (col("endLat") <= latNorth))


taxi=taxi.select('startLon','startLat','tip')


taxi.coalesce(1)\
  .write.format('com.databricks.spark.csv')\
  .options(header='true')\
  .save("/donnée_prétraité")
