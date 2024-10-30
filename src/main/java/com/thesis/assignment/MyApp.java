package com.thesis.assignment;

import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyApp {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port> <parallelism>");
			return;
		}
		String hostName = args[0];
		Integer port = Integer.parseInt(args[1]);
		Integer parallel = Integer.parseInt(args[2]);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallel);
		
		DataStream<String> text = env.socketTextStream(hostName, port);
		
		
		try {
			text.print();
			env.execute("Testing my app");
			
			//List<Integer> collect = amounts.filter(a -> a > threshold).reduce((integer, t1) -> integer + t1).collect();

			//System.out.println(collect);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
