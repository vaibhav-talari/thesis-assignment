package com.thesis.assignment;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DataPoint {
    public String houseId;// 1 - 10
    public long timestamp;
    public double powerReading;

    public DataPoint(){

    }
    public DataPoint(String id, long timestamp, double powerReading){
        this.houseId = id;
        this.timestamp = timestamp;
        this.powerReading = powerReading;

    }
    public String toString() {

        return this.houseId + ", " + timestamp_toDate(this.timestamp) + ", " + this.powerReading;

    }

    private String timestamp_toDate(long timestamp) {
        Instant instant = Instant.ofEpochMilli(timestamp);
        LocalDateTime localDateTime =
                LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedDateTime = localDateTime.format(formatter);
        return formattedDateTime;

    }


}
