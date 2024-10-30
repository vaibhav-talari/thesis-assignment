package com.thesis.assignment;

import java.util.stream.Stream;

public class Gen {

	public static void main(String[] args) {
		EnergyModel tt = new EnergyModel();
		Stream<String> streamGenerated = Stream.generate(tt::generateData);
		streamGenerated.limit(100000).forEach(System.out::println);
	}

}
