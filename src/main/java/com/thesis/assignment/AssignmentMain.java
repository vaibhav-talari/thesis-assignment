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
		
		if (args.length > 2) {
			System.err.println("USAGE:\nStreamingJob <parallelism degree> <no. of events>");
			return;
		}

		// set up the streaming execution environment, watermark intervall of 1 second
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setAutoWatermarkInterval(1000L);

		/* handle arguments
  		* If no arguments, default parallelism and a default of max long number of events
    		* If one argument, argument: parallelism degree, using default number of events
      		* If two arguments, argument 1: parallelism degree, argument 2: number events
		*/
		Long eventLimit = Long.MAX_VALUE;  //default
		
		if (args.length > 0) {
			Integer parallelism = Integer.parseInt(args[0]);
			env.setParallelism(parallelism);
			if( args.length == 2){
				eventLimit = Long.parseLong(args[1]);
			}
		}

		// Data source generates EmeterEvents stream of size eventLimit
		DataGeneratorSource<EMeterEvent> source = new DataGeneratorSource<>(new HouseDataGenerator(), eventLimit,
				TypeInformation.of(EMeterEvent.class));

		// set watermarking strategy
		DataStream<EMeterEvent> dataStream = env.fromSource(source, WatermarkStrategy
				.<EMeterEvent>forMonotonousTimestamps().withTimestampAssigner((element, ts) -> element.getTimestamp()),
				"energy meter events");
		// remove after testing
		//dataStream.print();

		DataStream<OperationContext> avgPowerStream = dataStream.keyBy(event -> event.getHouseId())
				.window(TumblingEventTimeWindows.of(Duration.ofHours(6))).apply(new HouseWindowOperation());
		
		// prints the per household average over the 6h tumbling windonw
		avgPowerStream.print();

		/* Defining a pattern with three simple conditions and one iterative condition
  		* Simple conditions keeps track of the latest three power averages for a house-id
    		* Iterative conditon compares the averages to look for three increasing averages in a row
		*/
		Pattern<OperationContext, ?> start = Pattern.<OperationContext>begin("first")
				.where(SimpleCondition.of(e -> e.getAveragePower() > 0)).next("second")
				.where(SimpleCondition.of(e -> e.getAveragePower() > 0)).next("third")
				.where(SimpleCondition.of(e -> e.getAveragePower() > 0)).next("fourth")
				.where(new IterativeCondition<OperationContext>() {

					private static final long serialVersionUID = 1L;

					@Override
					public boolean filter(OperationContext value, Context<OperationContext> ctx) throws Exception {

						OperationContext f = ctx.getEventsForPattern("first").iterator().next(); //event matching the first simple conditon
						OperationContext s = ctx.getEventsForPattern("second").iterator().next(); //event matching the second simple conditon
						OperationContext t = ctx.getEventsForPattern("third").iterator().next(); //event matching the third simple conditon

						// checking for three consecutive rising averages
						if ((value.getAveragePower() > t.getAveragePower())
								&& (t.getAveragePower() > s.getAveragePower())
								&& (s.getAveragePower() > f.getAveragePower())) {
							return true;
						} else {
							return false;
						}
					}
				});


		// CEP pattern initialization
		PatternStream<OperationContext> patternStream = CEP.pattern(avgPowerStream.keyBy(event -> event.getKey()),
				start);
		// Performs the matching with the created pattern
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
		// Prints results of pattern matching
		result.print();

		// execute job
		env.execute("Electricity Reading Stream");
	}
}
