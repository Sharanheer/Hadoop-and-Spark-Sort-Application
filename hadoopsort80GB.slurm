#!/bin/bash
#SBATCH --output=HadoopOutput80GB.log

javac -classpath $(hadoop classpath) MyHadoopTeraSort.java
jar cvf MyHadoopTeraSort.jar MyHadoop*.class


start_time=$SECONDS
hadoop jar MyHadoopTeraSort.jar MyHadoopTeraSort /input/data-80GB /user/schoudhari/output-hadoop
end_time=$SECONDS

running_time=$end_time-$start_time

echo 'Time taken to sort 80 GB: ' $running_time 'seconds' >HadoopSort80GB.log

hadoop jar /opt/hadoop-2.9.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.0.jar teravalidate /user/schoudhari/output-hadoop /user/schoudhari/report-hadoop


hadoop fs -get /user/schoudhari/report-hadoop/part-r-00000
