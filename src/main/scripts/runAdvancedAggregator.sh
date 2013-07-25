#! /bin/bash

export HLIB=`export TMP_HLIB= ; for jar in lib/*.jar; do export TMP_HLIB=$TMP_HLIB,file:////root/$jar; done ; echo $TMP_HLIB | sed 's/^,//g'`
hadoop fs -rm -r hdfs://hadoop-nn:8020/solar/daily
hadoop jar hadoop_tools-0.1.jar com.jeklsoft.hadoop.mr.AdvancedDailySolarAggregator "$HLIB" /solar/solar_and_wx /solar/daily
