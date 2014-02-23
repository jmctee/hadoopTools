DEFINE DATE_TIME_NORMALIZING_UDF DateTimeNormalizingUdf();

raw_solar = LOAD '$raw_solar'
    USING PigStorage(',') AS
        (account_id:chararray,
         time_of_measurement:chararray,
         cost:double,
         min_voltage:double,
         max_voltage:double,
         power:double);

raw_sunrise_sunset = LOAD '$raw_sunrise_sunset'
    USING PigStorage(',') AS
        (postal_code:chararray,
         day:chararray,
         sunrise:chararray,
         sunset:chararray);

raw_wx = LOAD '$raw_wx'
    USING PigStorage(',') AS
        (postal_code:chararray,
         time_of_measurement:chararray,
         temperature:double,
         precip:double,
         cloud_cover:double);

solar = FOREACH raw_solar
    GENERATE
        account_id AS account_id,
        DATE_TIME_NORMALIZING_UDF(time_of_measurement) AS time_of_measurement,
        ABS(power) AS power;

sunrise_sunset = FOREACH raw_sunrise_sunset
    GENERATE
        postal_code AS postal_code,
        DATE_TIME_NORMALIZING_UDF(day) AS day,
        DATE_TIME_NORMALIZING_UDF(sunrise) AS sunrise,
        DATE_TIME_NORMALIZING_UDF(sunset) AS sunset;

weather = FOREACH raw_wx
    GENERATE
        postal_code AS postal_code,
        DATE_TIME_NORMALIZING_UDF(time_of_measurement) AS time_of_measurement,
        temperature AS temperature,
        cloud_cover AS cloud_cover;

STORE solar INTO '$solar' USING PigStorage(',');

STORE sunrise_sunset INTO '$sunrise_sunset' USING PigStorage(',');

STORE weather INTO '$weather' USING PigStorage(',');
