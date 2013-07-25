package com.jeklsoft.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class HdfsFileCreator {

    private static final String NAME_NODE = "hadoop-nn";

    private static Configuration conf;
    private static FileSystem fs;
    private static FileSystem localFs;

    public HdfsFileCreator() throws Exception {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://" + NAME_NODE + ":8020/");

        fs = FileSystem.get(conf);

        localFs = FileSystem.getLocal(fs.getConf());
    }

    public void deletePath(Path target, boolean local) throws Exception {
        FileSystem fileSystem = local ? localFs : fs;

        if (fileSystem.exists(target)) {
            if (fileSystem.isDirectory(target)) {
                fileSystem.delete(target, true);
            }
            else {
                fileSystem.delete(target, false);
            }
        }
    }

    public void createHdfsFileFromLocalFile(Path source, Path destination, boolean overwrite) throws Exception {
        if (overwrite) {
            deletePath(destination, false);
        }

        fs.copyFromLocalFile(false, false, source, destination);
    }

    public void createLocalFileFromHdfsDir(Path source, Path destination, boolean overwrite) throws Exception {
        if (overwrite) {
            deletePath(destination, true);
        }

        FileUtil.copyMerge(fs, source, localFs, destination, false, conf, null);
    }

    public void createLocalFileFromHdfsFile(Path source, Path destination, boolean overwrite) throws Exception {
        if (overwrite) {
            deletePath(destination, true);
        }

        fs.copyToLocalFile(source, destination);
    }

    public static void main(String[] args) throws Exception {
        HdfsFileCreator creator = new HdfsFileCreator();

        boolean useDirectoryBasedFile = false;
        boolean overwrite = true;

        if (useDirectoryBasedFile) {
            creator.createHdfsFileFromLocalFile(new Path("src/main/resources/hourlySolar1.csv"), new Path("/solar/hourly/seg0"), overwrite);
            creator.createHdfsFileFromLocalFile(new Path("src/main/resources/hourlySolar2.csv"), new Path("/solar/hourly/seg1"), overwrite);

            creator.createLocalFileFromHdfsDir(new Path("/solar/hourly"), new Path("/tmp/hourly"), overwrite);
        }
        else {
            creator.createHdfsFileFromLocalFile(new Path("src/main/resources/hourlySolar1.csv"), new Path("/solar/hourly/seg0"), overwrite);

            creator.createLocalFileFromHdfsFile(new Path("/solar/hourly/seg0"), new Path("/tmp/hourly"), overwrite);
        }

        creator.createLocalFileFromHdfsDir(new Path("/solar/accounts"), new Path("/tmp/accounts"), overwrite);
    }
}
