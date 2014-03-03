package com.jeklsoft.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.jeklsoft.hadoop.domain.SolarReading;

public class BasicMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        SolarReading reading = new SolarReading(value.toString());

        String readingKey = String.format("%d,%s,%s,%.2f", reading.getId(), reading.getAccountId(), reading.getYyyymmdd(), reading.getSystemCapacity());

        output.collect(new Text(readingKey), new Text(reading.toString()));
    }
}
