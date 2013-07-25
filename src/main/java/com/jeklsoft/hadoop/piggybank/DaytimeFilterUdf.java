package com.jeklsoft.hadoop.piggybank;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

// TODO: This class currently is hardcoded to calculate sunrise and sunset based on values for 7/1/2013 in zip code 80013.
//       Need to flesh this out to use lookup table based on actual date and zip code.

public class DaytimeFilterUdf extends FilterFunc {

    private final static long sunriseMillisecondsFromMidnight = 20100000L;
    private final static long sunsetMillisecondsFromMidnight = 73860000L;

    private final String pathToSunriseSunsetData;

    public DaytimeFilterUdf(String pathToSunriseSunsetData) {
        this.pathToSunriseSunsetData = pathToSunriseSunsetData;
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        // Tuple has the following schema:
        //
        // id:long
        // account_id:chararray
        // postal_code:chararray
        // system_capacity:double
        // time_of_measurement:long <== This is the only field we're interested in
        // power:double

        Long time_of_measurement = (Long) input.get(4);

        Long localTime = new Long(new DateTime(time_of_measurement).toLocalTime().getMillisOfDay());

        return ((localTime >= sunriseMillisecondsFromMidnight) && (localTime <= sunsetMillisecondsFromMidnight));
    }
}
