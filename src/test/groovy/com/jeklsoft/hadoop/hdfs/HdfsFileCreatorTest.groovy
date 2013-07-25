package com.jeklsoft.hadoop.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.*

import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue

@Ignore
class HdfsFileCreatorTest {
  private static final String NAME_NODE = "hadoop-nn"

  private static final Boolean CLEANUP_ON_EXIT = true

  private static Configuration conf
  private static org.apache.hadoop.fs.FileSystem fs

  private HdfsFileCreator creator

  @BeforeClass
  public static void classSetup() throws IOException {
    conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://" + NAME_NODE + ":8020/")

    fs = org.apache.hadoop.fs.FileSystem.get(conf)
  }

  @AfterClass
  public static void classTeardown() {
    if (CLEANUP_ON_EXIT) {
      fs.delete(new Path("/data"), true)
    }
  }

  @Before
  public void setup() throws IOException {
    fs.delete(new Path("/data"), true)
    fs.mkdirs(new Path("/data"))
    creator = new HdfsFileCreator()
  }

  @Test
  public void testCreateHdfsFileFromLocalFile() throws IOException {
    assertFalse fs.exists(new Path("/data/file0"))

    creator.createHdfsFileFromLocalFile(new Path("src/test/resources/file0"), new Path("/data/file0"), true);

    assertTrue fs.exists(new Path("/data/file0"))
    assertTrue hdfsAndLocalFilesMatch(new Path("/data/file0"), new Path("src/test/resources/file0"))
  }

  @Test
  public void testCreateLocalFileFromHdfsFile() throws IOException {
    creator.createHdfsFileFromLocalFile(new Path("src/test/resources/file0"), new Path("/data/file0"), true);
    creator.createLocalFileFromHdfsFile(new Path("/data/file0"), new Path("/tmp/file0"), true)

    assertTrue localFilesMatch(new Path("src/test/resources/file0"), new Path("/tmp/file0"))
  }

  @Test
  public void testDeleteFromHdfs() throws IOException {
    assertFalse fs.exists(new Path("/data/file0"))

    creator.createHdfsFileFromLocalFile(new Path("src/test/resources/file0"), new Path("/data/file0"), true);

    assertTrue fs.exists(new Path("/data/file0"))

    creator.deletePath(new Path("/data/file0"), false)

    assertFalse fs.exists(new Path("/data/file0"))
  }

  @Test
  public void testCreateLocalFileFromHdfsDir() throws IOException {
    assertFalse fs.exists(new Path("/data/seg0"))
    assertFalse fs.exists(new Path("/data/seg1"))

    creator.createHdfsFileFromLocalFile(new Path("src/test/resources/file0"), new Path("/data/seg0"), true);
    creator.createHdfsFileFromLocalFile(new Path("src/test/resources/file1"), new Path("/data/seg1"), true);

    assertTrue fs.exists(new Path("/data/seg0"))
    assertTrue fs.exists(new Path("/data/seg1"))

    creator.createLocalFileFromHdfsDir(new Path("/data"), new Path("/tmp/combined"), true)

    assertTrue localFilesMatch(new Path("src/test/resources/combined"), new Path("/tmp/combined"))
  }

  @Test(expected = IOException.class)
  public void testCreateHdfsFileFromLocalFileOverwriteDisabled() throws IOException {
    assertFalse fs.exists(new Path("/data/file0"))

    creator.createHdfsFileFromLocalFile(new Path("src/test/resources/file0"), new Path("/data/file0"), true);

    assertTrue fs.exists(new Path("/data/file0"))

    creator.createHdfsFileFromLocalFile(new Path("src/test/resources/file0"), new Path("/data/file0"), false);

  }


  def localFilesMatch(def path1, def path2) {
    def file1 = new File(path1.toString())
    def file2 = new File(path2.toString())

    def string1 = file1.text
    def string2 = file2.text

    return string1 == string2
  }

  def hdfsAndLocalFilesMatch(def hdfsPath, def localPath) {
    def hdfsFile = new File("/tmp/hdfsFile")
    hdfsFile.delete();

    fs.copyToLocalFile(hdfsPath, new Path(hdfsFile.getAbsolutePath()))

    return localFilesMatch(new Path(hdfsFile.getAbsolutePath()), localPath)
  }
}
