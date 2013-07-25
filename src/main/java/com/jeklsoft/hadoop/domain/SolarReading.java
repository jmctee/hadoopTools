package com.jeklsoft.hadoop.domain;

public class SolarReading extends AbstractDomainObject {

    private Long id;
    private String accountId;
    private Double systemCapacity;
    private String yyyymmdd;
    private Long timeOfMeasurement;
    private Double power;
    private Double temperature;
    private Double cloudCover;

    public SolarReading(long id, String accountId, double systemCapacity, String yyyymmdd, long timeOfMeasurement, double power, double temperature, double cloudCover) {
        init(id, accountId, systemCapacity, yyyymmdd, timeOfMeasurement, power, temperature, cloudCover);
    }

    public SolarReading(String csv) {
        String[] fields = csv.split(",");

        if (fields.length != 8) {
            throw new RuntimeException("Invalid reading string received: " + csv);
        }

        init(Long.parseLong(fields[0]),
                fields[1],
                Double.parseDouble(fields[2]),
                fields[3],
                Long.parseLong(fields[4]),
                Double.parseDouble(fields[5]),
                Double.parseDouble(fields[6]),
                Double.parseDouble(fields[7]));
    }

    public String getReadingSubString() {
        StringBuilder reading = new StringBuilder();

        reading.append(timeOfMeasurement.toString()).append(",").append(power.toString()).append(",")
                .append(temperature.toString()).append(",").append(cloudCover.toString());

        return reading.toString();
    }

    private void init(long id, String accountId, double systemCapacity, String yyyymmdd, long timeOfMeasurement, double power, double temperature, double cloudCover) {
        this.id = id;
        this.accountId = accountId;
        this.systemCapacity = systemCapacity;
        this.yyyymmdd = yyyymmdd;
        this.timeOfMeasurement = timeOfMeasurement;
        this.power = power;
        this.temperature = temperature;
        this.cloudCover = cloudCover;
    }

    public long getId() {
        return id;
    }

    public String getAccountId() {
        return accountId;
    }

    public double getSystemCapacity() {
        return systemCapacity;
    }

    public String getYyyymmdd() {
        return yyyymmdd;
    }

    public long getTimeOfMeasurement() {
        return timeOfMeasurement;
    }

    public double getPower() {
        return power;
    }

    public double getTemperature() {
        return temperature;
    }

    public double getCloudCover() {
        return cloudCover;
    }
}
