package com.thesis.assignment;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class IncreasingAverageAlert {

	private Long key;
	private Double power;
	private Long windowStart;
	private Long windowEnd;

	public IncreasingAverageAlert() {
	}

	public IncreasingAverageAlert(Long key) {
		super();
		this.key = key;
	}

	public IncreasingAverageAlert(Long key, Double power, Long windowStart, Long windowEnd) {
		super();
		this.key = key;
		this.power = power;
		this.windowStart = windowStart;
		this.windowEnd = windowEnd;
	}

	public Long getKey() {
		return key;
	}

	public void setKey(Long key) {
		this.key = key;
	}

	public Double getPower() {
		return power;
	}

	public void setPower(Double power) {
		this.power = power;
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

	private String epochToDateTime(Long timestamp) {
		Instant instant = Instant.ofEpochMilli(timestamp);
		LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String formattedDateTime = localDateTime.format(formatter);

		return formattedDateTime;
	}

	@Override
	public String toString() {
		return "IncreasingAverageAlert [key=" + key + ", power=" + power + ", windowStart="
				+ epochToDateTime(windowStart) + ", windowEnd=" + epochToDateTime(windowEnd) + "]";
	}

}
