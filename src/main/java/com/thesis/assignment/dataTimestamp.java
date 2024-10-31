package com.thesis.assignment;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class dataTimestamp extends AscendingTimestampExtractor<DataPoint> {

    @Override
    public long extractAscendingTimestamp(DataPoint dataPoint) {
        return dataPoint.timestamp;
    }
}
