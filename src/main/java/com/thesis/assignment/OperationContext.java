package com.thesis.assignment;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class OperationContext {

	private Long key;
	private Long windowStart;
	private Long windowEnd;
	private Double averagePower;

	public OperationContext() {
	}

	public OperationContext(Long key, Long windowStart, Long windowEnd, Double averagePower) {
		super();
		this.key = key;
		this.windowStart = windowStart;
		this.windowEnd = windowEnd;
		this.averagePower = averagePower;
	}

	public Long getKey() {
		return key;
	}

	public void setKey(Long key) {
		this.key = key;
	}

	public Long getWindowStart() {
		return windowStart;
	}

	public void setWindowStart(Long windowStart) {
		this.windowStart = windowStart;
	}

	public Long getWindowEnd() {
		return windowEnd;
	}

	public void setWindowEnd(Long windowEnd) {
		this.windowEnd = windowEnd;
	}

	public Double getAveragePower() {
		return averagePower;
	}

	public void setAveragePower(Double averagePower) {
		this.averagePower = averagePower;
	}

	private String epochToDateTime(long timestamp) {
		Instant instant = Instant.ofEpochMilli(timestamp);
		LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String formattedDateTime = localDateTime.format(formatter);

		return formattedDateTime;
	}

	@Override
	public String toString() {
		return "OperationContext [key=" + key + ", windowStart=" + epochToDateTime(windowStart) + ", windowEnd="
				+ epochToDateTime(windowEnd) + ", averagePower=" + averagePower + "]";
	}

}
