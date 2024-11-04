package com.thesis.assignment;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Random;

import org.apache.flink.connector.datagen.source.GeneratorFunction;

public class HouseDataGenerator implements GeneratorFunction<Long, EMeterEvent> {

	private static final long serialVersionUID = 5119535009803546010L;
	private LocalDateTime baseTime = LocalDateTime.of(2018, Month.JANUARY, 1, 0, 0);
	private Random rand = new Random();

	@Override
	public EMeterEvent map(Long value) throws Exception {
		Long id = value % 10; // loops the id over 10 households
		Double power = 10.0 + rand.nextDouble() * 20; // generate random power reading

		// Increments the hour as all households have been processed
		if (id == 0) { 
			baseTime = baseTime.plusHours(1);
			Thread.sleep(10); //Limit the data generation speed
		}
	 
		long time = baseTime.toInstant(ZoneOffset.UTC).toEpochMilli();
		return new EMeterEvent(id, time, power);
	}

}
