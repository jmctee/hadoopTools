#! /bin/bash

set -x

hadoop fs -rm -r hdfs://hadoop-nn:8020/workflows
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows
hadoop fs -mkdir hdfs://hadoop-nn:8020/workflows/lib
hadoop fs -put build/output/lib/*.jar hdfs://hadoop-nn:8020/workflows/lib/
hadoop fs -put binaries/postgresql-9.1-903.jdbc4.jar hdfs://hadoop-nn:8020/workflows/lib/