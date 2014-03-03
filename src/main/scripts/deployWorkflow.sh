#! /bin/bash

set -x

scp src/main/resources/workflow.properties root@hadoop-nn:

hadoop fs -rm -r hdfs://hadoop-nn:8020/workflows/pig
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows/pig
hadoop fs -put src/main/pig/*.pig hdfs://hadoop-nn:8020/workflows/pig/

hadoop fs -rm -r hdfs://hadoop-nn:8020/workflows/hive
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows/hive
hadoop fs -put src/main/hive/*.hql hdfs://hadoop-nn:8020/workflows/hive/

hadoop fs -rm -r hdfs://hadoop-nn:8020/workflows/workflow
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows/workflow
hadoop fs -put src/main/workflows/workflow.xml hdfs://hadoop-nn:8020/workflows/workflow/

hadoop fs -rm hdfs://hadoop-nn:8020/workflows/lib/hadoop_tools-0.1.jar
hadoop fs -put build/libs/hadoop_tools-0.1.jar hdfs://hadoop-nn:8020/workflows/lib/

