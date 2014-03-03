package com.jeklsoft.hadoop.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ImageGeneratingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        StringBuilder readings = new StringBuilder();

        // TODO: Graph readings and create a PNG here and replace string generation...

        while (values.hasNext()) {
            String readingString = values.next().toString();

            String prefix = ",";
            if (readings.length() == 0) {
                prefix = "";
            }
            readings.append(prefix).append(readingString);
        }

        readings.insert(0, ",").insert(0, key.toString());

        output.collect(key, new Text(readings.toString()));
    }
}
