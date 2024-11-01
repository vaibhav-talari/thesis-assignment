package com.thesis.assignment;

import java.util.OptionalDouble;
import java.util.stream.StreamSupport;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HouseWindowOperation implements WindowFunction<EMeterEvent, OperationContext, Long, TimeWindow> {

	private static final long serialVersionUID = 3582672310114799476L;

	@Override
	public void apply(Long key, TimeWindow window, Iterable<EMeterEvent> input, Collector<OperationContext> out)
			throws Exception {

		OptionalDouble average = StreamSupport.stream(input.spliterator(), false).mapToDouble(EMeterEvent::getPower)
				.average();

		average.ifPresent(avg -> {
			OperationContext context = new OperationContext(key, window.getStart(), window.getEnd(), avg);
			out.collect(context);
		});
	}

}
