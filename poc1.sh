bash web_to_hdfs.sh
spark-submit --jars /lib/mongo-hadoop-spark-1.5.1.jar --driver-class-path /lib/mongo-hadoop-spark-1.5.1.jar poc1/travail.py
python3 data_vis.py 

