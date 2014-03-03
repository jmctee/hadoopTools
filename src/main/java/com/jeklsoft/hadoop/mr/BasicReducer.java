package com.jeklsoft.hadoop.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class BasicReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        StringBuilder readings = new StringBuilder();

        while (values.hasNext()) {
            String readingString = values.next().toString();

            String prefix = ",";
            if (readings.length() == 0) {
                prefix = "";
            }

            readings.append(prefix).append("(").append(readingString).append(")");
        }

        // The final record will have double parens at beginning and end of string, uncomment the following to
        // remove the unwanted characters.
        // if ("((".equals(readings.substring(0,2))) {
        //     readings.deleteCharAt(0).deleteCharAt(readings.length()-1);
        // }

        output.collect(key, new Text(readings.toString()));
    }
}
