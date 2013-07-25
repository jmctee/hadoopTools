hadoop fs -rm hdfs://hadoop-nn:8020/workflows/workflow.xml
hadoop fs -put src/main/workflows/workflow.xml hdfs://hadoop-nn:8020/workflows/

hadoop fs -rm hdfs://hadoop-nn:8020/workflows/pig/*.pig
hadoop fs -put src/main/pig/*.pig hdfs://hadoop-nn:8020/workflows/pig/

hadoop fs -rm hdfs://hadoop-nn:8020/workflows/lib/hadoop_tools-0.1.jar
hadoop fs -put build/libs/hadoop_tools-0.1.jar hdfs://hadoop-nn:8020/workflows/lib/

scp src/main/resources/workflow.properties root@hadoop-nn:
