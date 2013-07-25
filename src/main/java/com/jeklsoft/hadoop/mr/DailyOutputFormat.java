package com.jeklsoft.hadoop.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class DailyOutputFormat extends MultipleTextOutputFormat<Text, Text> {

    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String name) {
        String fileName = "";

        String[] fields = key.toString().split(",");

        // Store in path <id>/<yyyymmdd>
        fileName = String.format("%s/%s", fields[0], fields[2]);

        return fileName;
    }
}
