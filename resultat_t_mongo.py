import pymongo
from pyspark import SparkContext, SparkConf
import pymongo_spark
import findspark
findspark.init()
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
pymongo_spark.activate()
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans

conf = SparkConf().setAppName("resultat-mongo")
sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)
spark = SparkSession.builder.appName("resultat-mongo").getOrCreate()


#taxi= spark.read.format('csv').options(header='true').load('/d_pretraité/part-00000-41ded1db-06f0-498a-a393-9389df0061d5-c000.csv')
taxi = spark.read.csv('/d_pretraité/part-00000-41ded1db-06f0-498a-a393-9389df0061d5-c000.csv',inferSchema=True ,header=True,sep =',')
print("**********************************")
#taxi=taxi.rdd.map(lambda row: row.asDict())

vec_assembler = VectorAssembler(inputCols = taxi.columns, outputCol='features')
final_data = vec_assembler.transform(taxi)

kmeans = KMeans(featuresCol='features',k=12)
model = kmeans.fit(final_data)

centers = model.clusterCenters()
print("Cluster Centers: ")
A=[]
for center in centers:
    print(center.tolist())
    A.append(center.tolist())

resultat= sc.parallelize(A)\
.toDF(['startlon','startlat','tip'])


GA=resultat.rdd.map(lambda row: row.asDict())
GA.saveToMongoDB('mongodb://localhost:27017/POC1.Resultat')
