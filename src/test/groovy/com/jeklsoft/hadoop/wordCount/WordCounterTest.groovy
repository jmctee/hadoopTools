package com.jeklsoft.hadoop.wordCount

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.*

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue

@Ignore
public class WordCounterTest {

  private static final String NAME_NODE = "hadoop-nn"

  private static final Boolean CLEANUP_ON_EXIT = true

  private static final Map<String, Integer> BASIC_RESULT_MAP = ["Bye": 1, "Goodbye": 1, "Hadoop": 2, "Hello": 2, "World": 2]
  private static final String BASIC_HADOOP_COMMAND = "hadoop jar hadoop_tools-0.1.jar com.jeklsoft.hadoop.basicWordCount.WordCounter /data/input /data/output"
  private static final Map<String, Integer> ADVANCED_RESULT_MAP = ["Bye": 1, "Goodbye": 1, "Hadoop,": 1, "Hello": 2, "World!": 1, "World,": 1, "hadoop.": 1, "to": 1]
  private static final String ADVANCED_HADOOP_COMMAND1 = "hadoop jar hadoop_tools-0.1.jar com.jeklsoft.hadoop.advancedWordCount.AdvancedWordCounter /data/input /data/output"
  private static final Map<String, Integer> ADVANCED_WITH_PATTERN_RESULT_MAP = ["Bye": 1, "Goodbye": 1, "Hadoop": 1, "Hello": 2, "World": 2, "hadoop": 1]
  private static final String ADVANCED_HADOOP_COMMAND2 = "hadoop jar hadoop_tools-0.1.jar com.jeklsoft.hadoop.advancedWordCount.AdvancedWordCounter -Dwordcount.case.sensitive=true /data/input /data/output -skip /data/patterns"
  private static final Map<String, Integer> ADVANCED_WITH_PATTERN_CASE_INSENSITIVE_RESULT_MAP = ["bye": 1, "goodbye": 1, "hadoop": 2, "hello": 2, "world": 2]
  private static final String ADVANCED_HADOOP_COMMAND3 = "hadoop jar hadoop_tools-0.1.jar com.jeklsoft.hadoop.advancedWordCount.AdvancedWordCounter -Dwordcount.case.sensitive=false /data/input /data/output -skip /data/patterns"

  private static Configuration conf
  private static FileSystem fs

  @BeforeClass
  public static void classSetup() throws IOException {
    conf = new Configuration()
    conf.set("fs.defaultFS", "hdfs://" + NAME_NODE + ":8020/")
    conf.set("mapred.job.tracker", "hdfs://" + NAME_NODE + ":8021")
    conf.set("mapreduce.framework.name", NAME_NODE)

    fs = FileSystem.get(conf)
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
  public void testBasicWordCounter() throws IOException {
    setupBasicData()
    assertTrue(fs.getFileStatus(new Path("/data/input/file0")).isFile())
    assertTrue(fs.getFileStatus(new Path("/data/input/file1")).isFile())

    def command = ["ssh", "root@" + NAME_NODE, BASIC_HADOOP_COMMAND]
    def exitValue = runCommand(command)

    assertEquals(0, exitValue)

    Map<String, Integer> result = readFileIntoMap(new Path("/data/output/part-00000"))
    assertTrue compareMaps(BASIC_RESULT_MAP, result)
  }

  @Test
  public void testAdvancedWordCounter() throws IOException {
    setupAdvancedData()
    assertTrue(fs.getFileStatus(new Path("/data/input/file2")).isFile())
    assertTrue(fs.getFileStatus(new Path("/data/input/file3")).isFile())

    def command = ["ssh", "root@" + NAME_NODE, ADVANCED_HADOOP_COMMAND1]
    def exitValue = runCommand(command)

    assertEquals(0, exitValue)

    Map<String, Integer> result = readFileIntoMap(new Path("/data/output/part-00000"))
    assertTrue compareMaps(ADVANCED_RESULT_MAP, result)
  }

  @Test
  public void testAdvancedWordCounterWithPattern() throws IOException {
    setupAdvancedData()
    setupPatternData()
    assertTrue(fs.getFileStatus(new Path("/data/input/file2")).isFile())
    assertTrue(fs.getFileStatus(new Path("/data/input/file3")).isFile())
    assertTrue(fs.getFileStatus(new Path("/data/patterns")).isFile())

    def command = ["ssh", "root@" + NAME_NODE, ADVANCED_HADOOP_COMMAND2]
    def exitValue = runCommand(command)

    assertEquals(0, exitValue)

    Map<String, Integer> result = readFileIntoMap(new Path("/data/output/part-00000"))
    assertTrue compareMaps(ADVANCED_WITH_PATTERN_RESULT_MAP, result)
  }

  @Test
  public void testAdvancedWordCounterWithPatternAndCaseInsensitive() throws IOException {
    setupAdvancedData()
    setupPatternData()
    assertTrue(fs.getFileStatus(new Path("/data/input/file2")).isFile())
    assertTrue(fs.getFileStatus(new Path("/data/input/file3")).isFile())
    assertTrue(fs.getFileStatus(new Path("/data/patterns")).isFile())

    def command = ["ssh", "root@" + NAME_NODE, ADVANCED_HADOOP_COMMAND3]
    def exitValue = runCommand(command)

    assertEquals(0, exitValue)

    Map<String, Integer> result = readFileIntoMap(new Path("/data/output/part-00000"))
    assertTrue compareMaps(ADVANCED_WITH_PATTERN_CASE_INSENSITIVE_RESULT_MAP, result)
  }

  private void setupBasicData() throws IOException {
    fs.copyFromLocalFile(new Path("src/test/resources/file0"), new Path("/data/input/file0"))
    fs.copyFromLocalFile(new Path("src/test/resources/file1"), new Path("/data/input/file1"))
  }

  private void setupAdvancedData() throws IOException {
    fs.copyFromLocalFile(new Path("src/test/resources/file2"), new Path("/data/input/file2"))
    fs.copyFromLocalFile(new Path("src/test/resources/file3"), new Path("/data/input/file3"))
  }

  private void setupPatternData() throws IOException {
    fs.copyFromLocalFile(new Path("src/test/resources/patterns"), new Path("/data/patterns"))
  }

  private Map<String, Integer> readFileIntoMap(Path path) throws IOException {
    BufferedReader reader = null

    try {
      Map<String, Integer> result = new HashMap<String, Integer>()

      reader = new BufferedReader(new InputStreamReader(fs.open(path)))

      String line = reader.readLine()
      while (line != null) {
        String[] fields = line.split("\t")
        result.put(fields[0], new Integer(fields[1]))

        line = reader.readLine()
      }

      return result
    }
    finally {
      if (reader != null) {
        reader.close()
      }
    }
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

  boolean compareMaps(Map<String, Integer> expected, Map<String, Integer> actual) {
    def result = (expected.size() == actual.size())

    expected.each { k, v ->
      if (actual.containsKey(k)) {
        if (v != actual.get(k)) {
          result = false
        }
      }
      else {
        result = false
      }
    }
    result
  }

}
