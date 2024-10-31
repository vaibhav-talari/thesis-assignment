package com.thesis.assignment;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.OptionalDouble;
import java.util.stream.StreamSupport;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class LimitedEventTester {

	public static void main(String[] args) throws Exception {
		// Set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000L);

		// Add a custom source with a limited number of events
		DataStream<Event> limitedSourceStream = env.addSource(new LimitedEventSource(100))
				.assignTimestampsAndWatermarks(new EventMarker()); // limit to 5 events

		DataStream<Double> avgPower = limitedSourceStream.keyBy(point -> point.getId()).timeWindow(Time.hours(2))
				.apply(new ReadingAvg());

		avgPower.print();

		// Execute the job
		env.execute("Flink Limited Event Source Example");
	}

	public static class LimitedEventSource implements SourceFunction<Event> {
		private static final long serialVersionUID = 1L;
		private final int maxEvents;
		private volatile boolean isRunning = true;
		private LocalDateTime baseTime = LocalDateTime.of(2018, Month.JANUARY, 1, 0, 0);

		public LimitedEventSource(int maxEvents) {
			this.maxEvents = maxEvents;
		}

		@Override
		public void run(SourceContext<Event> ctx) throws Exception {
			int count = 0;
			while (isRunning && count < maxEvents) {

				int id = count % 3;

				if (count % 3 == 0) {
					//baseTime = baseTime.plusHours(1);
					baseTime = baseTime.plusMinutes(30);
				}
				long time = baseTime.toInstant(ZoneOffset.UTC).toEpochMilli();
				// System.out.println(time);
				ctx.collect(new Event(id, count * 2, time));
				count += 1;
				// Thread.sleep(10);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	static class Event {
		private int id;
		private int power;
		private long timestamp;

		public Event() {
		}

		public Event(int id, int power, long timestamp) {
			super();
			this.id = id;
			this.power = power;
			this.timestamp = timestamp;
		}

		public int getId() {
			return id;
		}

		public void setId(int id) {
			this.id = id;
		}

		public int getPower() {
			return power;
		}

		public void setPower(int power) {
			this.power = power;
		}

		public long getTimestamp() {
			return timestamp;
		}

		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public String toString() {
			return "Event [id=" + id + ", power=" + power + ", timestamp=" + epochToDateTime() + "]";
		}

		private String epochToDateTime() {
			Instant instant = Instant.ofEpochMilli(this.timestamp);
			LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("Z"));
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String formattedDateTime = localDateTime.format(formatter);

			return formattedDateTime;
		}

	}

	public static class ReadingAvg implements WindowFunction<Event, Double, Integer, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(Integer key, TimeWindow window, Iterable<Event> input, Collector<Double> out) {

			OptionalDouble average = StreamSupport.stream(input.spliterator(), false).mapToDouble(Event::getPower)
					.average();
			average.ifPresent(out::collect);

		}
	}

	public static class EventMarker extends AscendingTimestampExtractor<Event> {

		private static final long serialVersionUID = 1L;

		@Override
		public long extractAscendingTimestamp(Event event) {
			return event.getTimestamp();
		}
	}

}
