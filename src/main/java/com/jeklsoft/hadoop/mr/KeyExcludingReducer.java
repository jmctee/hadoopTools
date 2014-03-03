package com.jeklsoft.hadoop.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class KeyExcludingReducer extends MapReduceBase implements Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
        StringBuilder readings = new StringBuilder();

        while (values.hasNext()) {
            String readingString = values.next().toString();

            String prefix = ",";
            if (readings.length() == 0) {
                prefix = "";
            }
            readings.append(prefix).append(readingString);
        }

        readings.insert(0, ",").insert(0, key.toString());

        output.collect(NullWritable.get(), new Text(readings.toString()));
    }
}
