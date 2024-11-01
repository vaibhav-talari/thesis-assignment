package com.thesis.assignment;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.OptionalDouble;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class LimitedEventTester {

	public static void main(String[] args) throws Exception {
		long eventLimit = 30;
		// by default event time
		// 1 second interval for watermark
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setAutoWatermarkInterval(1000L);

		DataGeneratorSource<Event> source = new DataGeneratorSource<>(new EventGenerator(), eventLimit,
				TypeInformation.of(Event.class));

		DataStream<Event> limitedSourceStream = env.fromSource(source, WatermarkStrategy
				.<Event>forMonotonousTimestamps().withTimestampAssigner((element, ts) -> element.getTimestamp()),
				"limited event source");
		limitedSourceStream.print();

		DataStream<Double> avgPower = limitedSourceStream.keyBy(point -> point.getId())
				.window(TumblingEventTimeWindows.of(Duration.ofHours(2))).apply(new ReadingAvg());

		avgPower.print();

		// Execute the job
		env.execute("Flink Limited Event Source Example");
	}

	public static class EventGenerator implements GeneratorFunction<Long, Event> {

		static final long serialVersionUID = -7515880035579851892L;
		private static LocalDateTime baseTime = LocalDateTime.of(2018, Month.JANUARY, 1, 0, 0);

		@Override
		public Event map(Long value) throws Exception {
			int vala = value.intValue();

			int id = vala % 3;

			if (vala % 3 == 0) {
				// baseTime = baseTime.plusHours(1);
				baseTime = baseTime.plusMinutes(30);
			}
			long time = baseTime.toInstant(ZoneOffset.UTC).toEpochMilli();

			// Thread.sleep(10);
			return new Event(id, vala * 2, time);
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

		private static final long serialVersionUID = 3705289208219178286L;

		@Override
		public void apply(Integer key, TimeWindow window, Iterable<Event> input, Collector<Double> out) {

			OptionalDouble average = StreamSupport.stream(input.spliterator(), false).mapToDouble(Event::getPower)
					.average();
			average.ifPresent(out::collect);

		}
	}
}
