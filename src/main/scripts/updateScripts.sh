#! /bin/bash

set -x

ssh root@hadoop-nn 'rm -Rf scripts'
ssh root@hadoop-nn 'mkdir scripts'
scp src/main/scripts/*.* root@hadoop-nn:scripts/
ssh root@hadoop-nn 'rm -Rf pig'
ssh root@hadoop-nn 'mkdir pig'
scp src/main/pig/*.pig root@hadoop-nn:pig/
scp src/main/resources/pigCluster.properties root@hadoop-nn:pig/
scp src/main/resources/pigScript.properties root@hadoop-nn:pig/
hadoop fs -rm -R hdfs://hadoop-nn:8020/workflows
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows/lib
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows/pig
hadoop fs -put build/output/lib/*.jar hdfs://hadoop-nn:8020/workflows/lib/
