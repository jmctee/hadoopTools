package com.jeklsoft.hadoop.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.jeklsoft.hadoop.domain.SolarReading;

public class SolarReadingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        StringBuilder readings = new StringBuilder();

        while (values.hasNext()) {
            String readingString = values.next().toString();

            String prefix = ",";
            if (readings.length() == 0) {
                prefix = "";
            }
            SolarReading reading = new SolarReading(readingString);
            readings.append(prefix).append("(").append(reading.getReadingSubString()).append(")");
        }

        output.collect(key, new Text(readings.toString()));
    }
}
