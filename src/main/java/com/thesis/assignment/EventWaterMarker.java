package com.thesis.assignment;

import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class EventWaterMarker extends AscendingTimestampExtractor<EMeterEvent> {

	private static final long serialVersionUID = 1L;

	@Override
    public long extractAscendingTimestamp(EMeterEvent dataPoint) {
        return dataPoint.getTimestamp();
    }
}
