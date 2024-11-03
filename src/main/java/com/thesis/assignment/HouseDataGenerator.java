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
		Long id = value % 10;
		//Double power = 10 + rand.nextDouble() * 20;
		Double power = value * 2d;

		if (id == 0) {
			// baseTime = baseTime.plusHours(1);
			baseTime = baseTime.plusMinutes(30);
		}
		long time = baseTime.toInstant(ZoneOffset.UTC).toEpochMilli();
		return new EMeterEvent(id, time, power);
	}

}
