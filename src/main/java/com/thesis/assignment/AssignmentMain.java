package com.thesis.assignment;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public class AssignmentMain {

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("USAGE:\nStreamingJob <no. of events>");
			return;
		}

		Long eventLimit = Long.parseLong(args[0]);

		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setAutoWatermarkInterval(1000L);

		// Data source
		DataGeneratorSource<EMeterEvent> source = new DataGeneratorSource<>(new HouseDataGenerator(), eventLimit,
				TypeInformation.of(EMeterEvent.class));

		// set watermarking strategy
		DataStream<EMeterEvent> dataStream = env.fromSource(source, WatermarkStrategy
				.<EMeterEvent>forMonotonousTimestamps().withTimestampAssigner((element, ts) -> element.getTimestamp()),
				"energy meter events");
		//remove after testing
		dataStream.print();

		DataStream<Double> avgPower = dataStream.keyBy(event -> event.getHouseId())
				.window(TumblingEventTimeWindows.of(Duration.ofHours(2))).apply(new HouseWindowOperation());
		avgPower.print();
		
		// execute job
		env.execute("Electricity Reading Stream");
	}
}
