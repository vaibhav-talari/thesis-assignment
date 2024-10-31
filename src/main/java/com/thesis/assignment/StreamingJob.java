package com.thesis.assignment;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000L); // 1000L is temporary

		DataStream<EMeterEvent> dataStream = env.addSource(new HousehDataGen())
				.assignTimestampsAndWatermarks(new EventWaterMarker());

		DataStream<EMeterEvent> avgPower = dataStream.keyBy(point -> point.getHouseId()).timeWindow(Time.hours(6))
				.apply(new ReadingAvg());

		// execute program
		avgPower.print();
		env.execute("Flink Streaming Java API Test");
	}

	public static class ReadingAvg implements WindowFunction<EMeterEvent, EMeterEvent, String, TimeWindow> {

		private static final long serialVersionUID = 1L;

		@Override
		public void apply(String houseId, TimeWindow timeWindow, Iterable<EMeterEvent> input,
				Collector<EMeterEvent> out) {

			int count = 0;
			double sum = 0.0;
			for (EMeterEvent event : input) {
				count++;
				sum += event.getPower();
			}
			double avgPow = sum / count;

			// emit a SensorReading with the average temperature
			out.collect(new EMeterEvent(houseId, timeWindow.getEnd(), avgPow));

		}
	}
}
