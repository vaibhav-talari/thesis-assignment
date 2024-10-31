package com.thesis.assignment;

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

        return this.houseId + ", " + this.timestamp + ", " + this.powerReading;

    }


}