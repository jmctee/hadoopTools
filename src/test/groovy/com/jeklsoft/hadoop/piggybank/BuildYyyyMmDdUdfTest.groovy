package com.jeklsoft.hadoop.piggybank

import org.apache.pig.data.DefaultTuple
import org.joda.time.DateTime
import org.junit.Test

import static org.junit.Assert.assertEquals

class BuildYyyyMmDdUdfTest {

  @Test
  public void testValidDateTime() throws Exception {
    Long time = new DateTime("2013-07-03T05:35:00.000").millis
    Long expected = 20130703L

    BuildYyyyMmDdUdf udf = new BuildYyyyMmDdUdf();

    org.apache.pig.data.Tuple tuple = new DefaultTuple()
    tuple.append(time)

    Long result = udf.exec(tuple)

    assertEquals("Unexpected YYYMMDD value received", expected, result)
  }

  @Test
  public void testInvalidDateTime() throws Exception {
    Long expected = null

    BuildYyyyMmDdUdf udf = new BuildYyyyMmDdUdf();

    org.apache.pig.data.Tuple tuple = new DefaultTuple()
    tuple.append("0") // String is not a valid long

    Long result = udf.exec(tuple)

    assertEquals("Invalid date input dod not result in null", expected, result)
  }
}
