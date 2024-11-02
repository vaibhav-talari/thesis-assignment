package com.thesis.assignment;

import java.time.Duration;
import java.util.Iterator;
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
		Pattern<OperationContext, ?> start = Pattern.<OperationContext>begin("start")
				.where(new SimpleCondition<OperationContext>() {

					private static final long serialVersionUID = 1644697500047419226L;

					@Override
					public boolean filter(OperationContext value) throws Exception {
						return value.getAveragePower() > 0;
					}

				}).next("middle").where(new IterativeCondition<OperationContext>() {

					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(OperationContext value, Context<OperationContext> ctx) throws Exception {
						int eventCount = 0;
						int simchec = 0;
						Iterator<OperationContext> iterator = ctx.getEventsForPattern("start").iterator();
						OperationContext current = value;
						// System.out.println("current" + previous);
						while (iterator.hasNext()) {
							simchec += 1;
							OperationContext previous = iterator.next();
							/// System.out.println("check" + current);
							if (Double.compare(current.getAveragePower(), previous.getAveragePower()) > 0) {
								eventCount += 1;
								current = previous;
							} else {
								break;
							}
						}
						// System.out.println("e count" + eventCount);
						// System.out.println("total" + simchec);
						if ((eventCount < 0) || (eventCount >= 1)) {
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
						match.get("middle").forEach(dat -> {
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
