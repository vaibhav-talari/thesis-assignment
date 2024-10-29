package com.thesis.assignment;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Random;
import java.util.stream.Stream;

public class Gen {

	public static void main(String[] args) {
		DataPoint tt = new DataPoint();
		Stream<String> streamGenerated = Stream.generate(tt::generateData);
		streamGenerated.limit(100000).forEach(System.out::println);
	}

}

class DataPoint {
	private int houseId;// 1 - 10
	private LocalDateTime timestamp = LocalDateTime.of(2018, Month.JUNE, 25, 0, 0);
	private float powerReading;
	private int counter = 0;
	Random r = new Random();

	public String generateData() {

		houseId = counter % 10;
		powerReading = 10 + r.nextFloat() * 20;
		if (counter % 10 == 0) {
			timestamp = timestamp.plusHours(1);
			System.out.println();
		}

		counter += 1;
		return "id: " + houseId + " time: " + timestamp + " reading: " + powerReading;

	}

}
