#!/bin/bash
#SBATCH --output=SparkOutput20GB.log

javac -classpath /opt/spark-2.3.0-bin-hadoop2.7/jars/spark-core_2.11-2.3.0.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/spark-sql_2.11-2.3.0.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/scala-compiler-2.11.8.jar:/opt/spark-2.3.0-bin-hadoop2.7/jars/scala-library-2.11.8.jar SparkSort.java
jar cvf SparkSort.jar SparkSort*.class


start_time=$SECONDS

spark-submit --class SparkSort --master yarn --deploy-mode client --driver-memory 1g --executor-memory 1g --executor-cores 4 --num-executors 4 SparkSort.jar /input/data-20GB /user/schoudhari/output-spark

end_time=$SECONDS

running_time=$end_time-$start_time

echo 'Time taken to sort 20 GB: ' $running_time 'seconds' >SparkSort20GB.log

hadoop jar /opt/hadoop-2.9.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.0.jar teravalidate /user/schoudhari/output-spark /user/schoudhari/report-spark


hadoop fs -get /user/schoudhari/report-spark/part-r-00000
