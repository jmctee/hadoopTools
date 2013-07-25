package com.jeklsoft.hadoop.workflowHelpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileCopier {

    public FileCopier(String nameNode, Path source, Path destination) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", nameNode);
        FileSystem fs = FileSystem.get(conf);

        fs.copyFromLocalFile(false, false, source, destination);
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            throw new RuntimeException("Invalid arguments, expected <name-node> <source> <destination>, got " + args);
        }

        new FileCopier(args[0], new Path(args[1]), new Path(args[2]));
    }
}
