DEFINE IS_DAYTIME_READING DaytimeFilterUdf('path_to_sunrise_sunset_data');
DEFINE BUILD_YYYYMMDD BuildYyyyMmDdUdf();

accounts = LOAD '$accounts'
    USING PigStorage(',') AS
        (id:long,
        account_identifier:chararray,
        name:chararray,
        address1:chararray,
        address2:chararray,
        city:chararray,
        state:chararray,
        postal_code:chararray,
        system_capacity:double,
        last_updated:chararray);

solar = LOAD '$solar'
    USING PigStorage(',') AS
        (account_id:chararray,
         time_of_measurement:long,
         power:double);

weather = LOAD '$weather'
    USING PigStorage(',') AS
        (postal_code:chararray,
         time_of_measurement:long,
         temperature:double,
         cloud_cover:double);

summary_accounts = FOREACH accounts
    GENERATE
        id AS id,
        account_identifier AS account_id,
        postal_code AS postal_code,
        system_capacity AS system_capacity;

joined_accounts_with_solar = JOIN summary_accounts BY account_id, solar BY account_id;

accounts_with_solar = FOREACH joined_accounts_with_solar
    GENERATE
        id AS id,
        summary_accounts::account_id AS account_id,
        postal_code AS postal_code,
        system_capacity AS system_capacity,
        time_of_measurement AS time_of_measurement,
        power AS power;

daytime_solar_readings = FILTER accounts_with_solar BY IS_DAYTIME_READING(*);

joined_solar_and_wx = JOIN daytime_solar_readings BY (postal_code, time_of_measurement), weather BY (postal_code, time_of_measurement);

solar_and_wx = FOREACH joined_solar_and_wx
    GENERATE
        id AS id,
        account_id AS account_id,
        system_capacity AS system_capacity,
        BUILD_YYYYMMDD(daytime_solar_readings::time_of_measurement) AS yyyymmdd,
        daytime_solar_readings::time_of_measurement AS time_of_measurement,
        power AS power,
        temperature AS temperature,
        cloud_cover AS cloud_cover;

-- Defect #1234 - input data was found to have duplicate records, need to clean them.
distinct_solar_and_wx = DISTINCT solar_and_wx;

STORE distinct_solar_and_wx INTO '$solar_and_wx' USING PigStorage(',');

groupedByDay = GROUP distinct_solar_and_wx BY (id, yyyymmdd);

sortedAndGroupedByDay = FOREACH groupedByDay {
        sortedDailyData = ORDER distinct_solar_and_wx BY time_of_measurement;
        GENERATE group, sortedDailyData;
    };

-- Example of running:
-- DESCRIBE sortedAndGroupedByDay;
-- sortedAndGroupedByDay: {group: (id: long,yyyymmdd: long),sortedDailyData: {(id: long,account_id: chararray,system_capacity: double,
--                                                                             yyyymmdd: long,time_of_measurement: long,power: double,
--                                                                             temperature: double,cloud_cover: double)

STORE sortedAndGroupedByDay INTO '$solar_and_wx_grouped_and_sorted_by_day' USING PigStorage(',');
