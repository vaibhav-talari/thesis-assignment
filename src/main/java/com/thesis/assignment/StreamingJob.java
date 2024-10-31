package com.thesis.assignment;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a full example of a Flink Streaming Job, see the SocketTextStreamWordCount.java
 * file in the same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster.
 * Just type
 * 		mvn clean package
 * in the projects root directory.
 * You will find the jar in
 * 		target/flink-assignment-1.0-SNAPSHOT.jar
 * From the CLI you can then run
 * 		./bin/flink run -c com.thesis.assignment.StreamingJob target/flink-assignment-1.0-SNAPSHOT.jar
 *
 * For more information on the CLI see:
 *
 * http://flink.apache.org/docs/latest/apis/cli.html
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().setAutoWatermarkInterval(1000L); //1000L is temporary


		DataStream<DataPoint> dataStream =env
				.addSource(new HousehDataGen())
				.assignTimestampsAndWatermarks(new dataTimestamp());

		DataStream<DataPoint> avgPower = dataStream
				.keyBy(point -> point.houseId)
				.timeWindow(Time.hours(6)).
				apply(new ReadingAvg());

		/**
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		avgPower.print();
		env.execute("Flink Streaming Java API Test");
	}
	public static class ReadingAvg implements WindowFunction<DataPoint,DataPoint,String,TimeWindow> {


		@Override
		public void apply(String houseId, TimeWindow timeWindow, Iterable<DataPoint> input, Collector<DataPoint> out) {

			int count = 0;
			double sum = 0.0;
			for (DataPoint point : input) {
				count++;
				sum += point.powerReading;
			}
			double avgPow = sum / count;

			// emit a SensorReading with the average temperature
			out.collect(new DataPoint(houseId, timeWindow.getEnd(), avgPow));


		}
	}
}

