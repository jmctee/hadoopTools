package com.jeklsoft.hadoop.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import com.jeklsoft.hadoop.domain.SolarReading;

// TODO: This pattern, using JobConf, is deprecated.
//       See http://stackoverflow.com/questions/8603788/hadoop-jobconf-class-is-deprecated-need-updated-example
//       Need to review and refactor.

public class BasicDailySolarAggregator {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

            SolarReading reading = new SolarReading(value.toString());

            String readingKey = String.format("%d,%s,%s,%.2f", reading.getId(), reading.getAccountId(), reading.getYyyymmdd(), reading.getSystemCapacity());

            output.collect(new Text(readingKey), new Text(reading.toString()));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
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

// Step 2: The final record will have double parens at beginning and end of string, uncomment the following to remove
//         the unwanted characters.
//            if ("((".equals(readings.substring(0,2))) {
//                readings.deleteCharAt(0).deleteCharAt(readings.length()-1);
//            }

            output.collect(key, new Text(readings.toString()));
        }
    }

    public static JobConf configure(String jars, Path inputPath, Path outputPath) {
        JobConf conf = new JobConf(AdvancedDailySolarAggregator.class);
        conf.setJobName("BasicDailySolarAggregator");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.set("tmpjars", jars);

// Step 3: Uncomment to change the key/value separator from tab to comma
//        conf.set("mapred.textoutputformat.separator",",");

        FileInputFormat.setInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);

        return conf;
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = BasicDailySolarAggregator.configure(args[0], new Path(args[1]), new Path(args[2]));

        JobClient.runJob(conf);
    }
}
