package com.thesis.assignment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.Collector;

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
		// remove after testing
		dataStream.print();

		DataStream<OperationContext> avgPowerStream = dataStream.keyBy(event -> event.getHouseId())
				.window(TumblingEventTimeWindows.of(Duration.ofHours(2))).apply(new HouseWindowOperation());
		avgPowerStream.print();

		Pattern<OperationContext, ?> start = Pattern.<OperationContext>begin("start").where(new SimpleCondition<>() {

			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(OperationContext value) {
				return value.getAveragePower() > 20;
			}
		});

		PatternStream<OperationContext> patternStream = CEP.pattern(avgPowerStream, start);
		System.out.println();

		DataStream<CEPContext> result = patternStream
				.process(new PatternProcessFunction<OperationContext, CEPContext>() {

					private static final long serialVersionUID = 2556726478885746626L;

					@Override
					public void processMatch(Map<String, List<OperationContext>> match, Context ctx,
							Collector<CEPContext> out) throws Exception {
						out.collect(new CEPContext(match.get("start")));
					}
				});
		result.print();

		// execute job
		env.execute("Electricity Reading Stream");
	}
}
