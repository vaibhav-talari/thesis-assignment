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
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
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

		// .timesOrMore(3).greedy()
		Pattern<OperationContext, ?> start = Pattern.<OperationContext>begin("first")
				.where(SimpleCondition.of(e -> e.getAveragePower() > 0)).next("second")
				.where(SimpleCondition.of(e -> e.getAveragePower() > 0)).next("third")
				.where(SimpleCondition.of(e -> e.getAveragePower() > 0)).next("fourth")
				.where(new IterativeCondition<OperationContext>() {

					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(OperationContext value, Context<OperationContext> ctx) throws Exception {
						//System.out.println("current" + value);
						//System.out.println("first");
						//ctx.getEventsForPattern("first").forEach(System.out::println);
						//System.out.println("second");
						//ctx.getEventsForPattern("second").forEach(System.out::println);
						//System.out.println("third");
						//ctx.getEventsForPattern("third").forEach(System.out::println);

						OperationContext f = ctx.getEventsForPattern("first").iterator().next();
						OperationContext s = ctx.getEventsForPattern("second").iterator().next();
						OperationContext t = ctx.getEventsForPattern("third").iterator().next();

						if ((value.getAveragePower() > t.getAveragePower())
								&& (t.getAveragePower() > s.getAveragePower())
								&& (s.getAveragePower() > f.getAveragePower())) {
							return true;
						} else {
							return false;
						}
					}
				});

		// SimpleCondition.of(event -> event.getAveragePower() > 20)

		// avgPowerStream should be key
		PatternStream<OperationContext> patternStream = CEP.pattern(avgPowerStream.keyBy(event -> event.getKey()),
				start);
		// System.out.println();

		DataStream<IncreasingAverageAlert> result = patternStream
				.process(new PatternProcessFunction<OperationContext, IncreasingAverageAlert>() {

					private static final long serialVersionUID = 2556726478885746626L;

					@Override
					public void processMatch(Map<String, List<OperationContext>> match, Context ctx,
							Collector<IncreasingAverageAlert> out) throws Exception {
						match.get("fourth").forEach(dat -> {
							out.collect(new IncreasingAverageAlert(dat.getKey(), dat.getAveragePower(),
									dat.getWindowStart(), dat.getWindowEnd()));
						});
					}
				});
		result.print();

		// execute job
		env.execute("Electricity Reading Stream");
	}
}
