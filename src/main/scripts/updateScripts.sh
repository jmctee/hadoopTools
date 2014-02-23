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

ssh root@hadoop-nn 'rm -Rf hive'
ssh root@hadoop-nn 'mkdir hive'
scp src/main/hive/*.hql root@hadoop-nn:hive/

ssh root@hadoop-nn 'rm -Rf lib'
ssh root@hadoop-nn 'mkdir lib'
scp build/output/lib/*.jar root@hadoop-nn:lib/

ssh root@hadoop-nn 'rm -Rf .groovy'
ssh root@hadoop-nn 'mkdir .groovy'
ssh root@hadoop-nn 'mkdir .groovy/lib'
scp binaries/postgresql-9.1-903.jdbc4.jar root@hadoop-nn:.groovy/lib/

ssh root@hadoop-nn 'rm -Rf resources'
ssh root@hadoop-nn 'mkdir resources'
scp -r src/main/resources root@hadoop-nn:

hadoop fs -rm -R hdfs://hadoop-nn:8020/workflows
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows/lib
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows/pig
hadoop fs -put build/output/lib/*.jar hdfs://hadoop-nn:8020/workflows/lib/
