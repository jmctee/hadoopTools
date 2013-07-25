package com.jeklsoft.hadoop.piggybank

import org.apache.pig.data.DefaultTuple
import org.apache.pig.data.Tuple
import org.joda.time.DateTime
import org.junit.Test

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNull

class DateTimeNormalizingUdfTest {

  @Test
  public void testFormat1() throws Exception {
    String dateString = "07/31/2013 23:00:00"
    Long expected = new DateTime("2013-07-31T23:00:00.000-06:00").millis

    Tuple tuple = new DefaultTuple()

    tuple.append(dateString)

    DateTimeNormalizingUdf udf = new DateTimeNormalizingUdf()

    Long result = udf.exec(tuple)

    assertEquals("Format 1 failed", expected, result)
  }

  @Test
  public void testFormat2() throws Exception {
    String dateString = "2013/07/31"
    Long expected = new DateTime("2013-07-31T00:00:00.000-06:00").millis

    Tuple tuple = new DefaultTuple()

    tuple.append(dateString)

    DateTimeNormalizingUdf udf = new DateTimeNormalizingUdf()

    Long result = udf.exec(tuple)

    assertEquals("Format 2 failed", expected, result)
  }

  @Test
  public void testFormat3() throws Exception {
    String dateString = "20:31"
    Long expected = ((20 * 60) + 31) * 60 * 1000;

    Tuple tuple = new DefaultTuple()

    tuple.append(dateString)

    DateTimeNormalizingUdf udf = new DateTimeNormalizingUdf()

    Long result = udf.exec(tuple)

    assertEquals("Format 3 failed", expected, result)
  }

  @Test
  public void testInvalidFormat1() throws Exception {
    String dateString = "ZZZ"

    Tuple tuple = new DefaultTuple()

    tuple.append(dateString)

    DateTimeNormalizingUdf udf = new DateTimeNormalizingUdf()

    Long result = udf.exec(tuple)
    assertNull("Invalid format 1 did not return null", result)
  }

  @Test
  public void testInvalidFormat2() throws Exception {
    String dateString = "10:2Z"

    Tuple tuple = new DefaultTuple()

    tuple.append(dateString)

    DateTimeNormalizingUdf udf = new DateTimeNormalizingUdf()

    Long result = udf.exec(tuple)
    assertNull("Invalid format 2 did not return null", result)
  }

  @Test
  public void testInvalidFormat3() throws Exception {
    String dateString = "10:20:01"

    Tuple tuple = new DefaultTuple()

    tuple.append(dateString)

    DateTimeNormalizingUdf udf = new DateTimeNormalizingUdf()

    Long result = udf.exec(tuple)
    assertNull("Invalid format 3 did not return null", result)
  }
}
