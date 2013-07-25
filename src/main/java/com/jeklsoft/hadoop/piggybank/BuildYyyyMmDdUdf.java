package com.jeklsoft.hadoop.piggybank;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;

public class BuildYyyyMmDdUdf extends EvalFunc<Long> {
    @Override
    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        Long result = null;

        try {
            Long millis = (Long) input.get(0);

            DateTime dateTime = new DateTime(millis);

            int year = dateTime.year().get();
            int month = dateTime.monthOfYear().get();
            int day = dateTime.dayOfMonth().get();

            String yyyymmddString = String.format("%04d%02d%02d", year, month, day);

            result = Long.parseLong(yyyymmddString);
        }
        catch (Exception e) {
            // Returning null will indicate to Pig that we failed but we want to continue execution.
            // Don't throw exception!
            warn("Invalid date-time value " + input.get(0) + "Received error: " + e.getMessage(), PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED);
        }

        return result;
    }
}
