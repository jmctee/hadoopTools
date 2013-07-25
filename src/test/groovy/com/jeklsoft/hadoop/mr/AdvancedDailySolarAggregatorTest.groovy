package com.jeklsoft.hadoop.mr

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.junit.*

@Ignore
class AdvancedDailySolarAggregatorTest {
  private static final String NAME_NODE = "hadoop-nn"

  private static final Boolean CLEANUP_ON_EXIT = false

  private static Configuration conf
  private static org.apache.hadoop.fs.FileSystem fs

  @BeforeClass
  public static void classSetup() throws IOException {
    conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://" + NAME_NODE + ":8020/")
    conf.set("mapred.job.tracker", "hdfs://" + NAME_NODE + ":8021")
    conf.set("mapreduce.framework.name", NAME_NODE)

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
    fs.delete(new Path("/data/input"), true)
    fs.delete(new Path("/data/output"), true)
    fs.delete(new Path("/data/patterns"), true)

    fs.mkdirs(new Path("/data/input"))
  }

  @Test
  public void testAggregation() throws IOException {
    String COMMAND = "hadoop jar -libjars /root/lib/* hadoop_tools-0.1.jar com.jeklsoft.hadoop.mr.AdvancedDailySolarAggregator /solar/solar_and_wx /solar/daily"

    def command = ["ssh", "root@" + NAME_NODE, COMMAND]
    def exitValue = runCommand(command)

    println exitValue

//    setupBasicData()
//    assertTrue(fs.getFileStatus(new Path("/data/input/file0")).isFile())
//    assertTrue(fs.getFileStatus(new Path("/data/input/file1")).isFile())
//
//    def command = ["ssh", "root@" + NAME_NODE, BASIC_HADOOP_COMMAND]
//    def exitValue = runCommand(command)
//
//    assertEquals(0, exitValue)
//
//    Map<String, Integer> result = readFileIntoMap(new Path("/data/output/part-00000"))
//    assertTrue compareMaps(BASIC_RESULT_MAP, result)
  }

  private def runCommand(def command, def storeOutput = false) {
    def proc = command.execute()
    proc.waitFor()

    if (storeOutput) {
      def file = new File("processResults")
      file.write("return code: ${ proc.exitValue()}\n")
      file.append("stderr: ${proc.err.text}\n")
      file.append("stdout: ${proc.in.text}\n")
    }

    proc.exitValue()
  }
}
