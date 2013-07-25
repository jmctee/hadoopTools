package com.jeklsoft.hadoop.piggybank

import org.apache.pig.data.DefaultTuple
import org.apache.pig.data.Tuple
import org.joda.time.DateTime
import org.junit.Test

import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertTrue

class DaytimeFilterUdfTest {

  @Test
  public void sunriseIsDaytime() throws Exception {
    Long time = new DateTime("2013-07-02T05:35:00.000").millis

    Tuple tuple = createTuple(time)

    DaytimeFilterUdf udf = new DaytimeFilterUdf("")

    Boolean result = udf.exec(tuple)

    assertTrue(result)
  }

  @Test
  public void oneSecondBeforeSunriseIsNotDaytime() throws Exception {
    Long time = new DateTime("2013-07-02T05:34:59.000").millis

    Tuple tuple = createTuple(time)

    DaytimeFilterUdf udf = new DaytimeFilterUdf("")

    Boolean result = udf.exec(tuple)

    assertFalse(result)
  }

  @Test
  public void sunsetIsDaytime() throws Exception {
    Long time = new DateTime("2013-07-02T20:31:00.000").millis

    Tuple tuple = createTuple(time)

    DaytimeFilterUdf udf = new DaytimeFilterUdf("")

    Boolean result = udf.exec(tuple)

    assertTrue(result)
  }

  @Test
  public void oneSecondAfterSunsetIsNotDaytime() throws Exception {
    Long time = new DateTime("2013-07-02T20:31:01.000").millis

    Tuple tuple = createTuple(time)

    DaytimeFilterUdf udf = new DaytimeFilterUdf("")

    Boolean result = udf.exec(tuple)

    assertFalse(result)
  }

  private Tuple createTuple(long time) {
    Tuple tuple = new DefaultTuple()
    tuple.append(1L)
    tuple.append("account_id")
    tuple.append("80013")
    tuple.append("7.56")
    tuple.append(time)
    tuple.append(5.04 as Double)
    tuple
  }
}
