package com.jeklsoft.hadoop.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

public class AdvancedDailySolarMultiOutput {

    public static JobConf configure(String jars, Path inputPath, Path outputPath) {
        JobConf conf = new JobConf(AdvancedDailySolarAggregator.class);
        conf.setJobName("AdvancedDailySolarMultiOutput");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(BasicMapper.class);
        conf.setCombinerClass(SolarReadingReducer.class);
        conf.setReducerClass(ImageGeneratingReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(DailyOutputFormat.class);

        conf.set("tmpjars", jars);

        FileInputFormat.setInputPaths(conf, inputPath);
        FileOutputFormat.setOutputPath(conf, outputPath);

        return conf;
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = AdvancedDailySolarMultiOutput.configure(args[0], new Path(args[1]), new Path(args[2]));

        JobClient.runJob(conf);
    }
}
