package com.jeklsoft.hadoop.piggybank;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.Tuple;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateTimeNormalizingUdf extends EvalFunc<Long> {

    @Override
    public Long exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        String dateTimeString = (String) input.get(0);

        Long dateTime = null;

        try {
            // 07/31/2013 23:00:00
            DateTimeFormatter formatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");
            dateTime = formatter.parseMillis(dateTimeString);
        }
        catch (Exception e) {
            try {
                // 2013/07/01
                DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd");
                dateTime = formatter.parseMillis(dateTimeString);
            }
            catch (Exception e1) {
                try {
                    // 20:31
                    String[] fields = dateTimeString.split(":");

                    if (fields.length == 2) {
                        dateTime = new Long((Integer.parseInt(fields[0]) * 60 + Integer.parseInt(fields[1])) * 60 * 1000);
                    }
                    else {
                        // Returning null will indicate to Pig that we failed but we want to continue execution.
                        // Don't throw exception!
                        warn("Invalid date-time string " + dateTimeString + "Received error: " + e.getMessage(), PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED);
                    }
                }
                catch (Exception e2) {
                    // Returning null will indicate to Pig that we failed but we want to continue execution.
                    // Don't throw exception!
                    warn("Invalid date-time string " + dateTimeString + "Received error: " + e.getMessage(), PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED);
                }
            }
        }

        return dateTime;
    }
}
