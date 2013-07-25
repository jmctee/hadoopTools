package com.jeklsoft.hadoop.domain

import org.junit.Test

class SolarReadingTest {
  @Test
  public void test() throws Exception {
    def reading = new SolarReading("3,33-333-3,7.56,20130701,1372680000000,0.158,13.9,0.5")

    println reading
  }
}
