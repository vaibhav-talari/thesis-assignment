package com.thesis.assignment;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class EMeterEvent {
	private String houseId;
	private long timestamp;
	private double power;

	public EMeterEvent() {
	}

	public EMeterEvent(String id, long timestamp, double power) {
		this.houseId = id;
		this.timestamp = timestamp;
		this.power = power;

	}

	public String getHouseId() {
		return houseId;
	}

	public void setHouseId(String houseId) {
		this.houseId = houseId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public double getPower() {
		return power;
	}

	public void setPower(double power) {
		this.power = power;
	}

	private String epochToDateTime() {
		Instant instant = Instant.ofEpochMilli(this.timestamp);
		LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"));
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String formattedDateTime = localDateTime.format(formatter);

		return formattedDateTime;
	}

	@Override
	public String toString() {
		return this.houseId + ", " + epochToDateTime() + ", " + this.power;
	}

}
