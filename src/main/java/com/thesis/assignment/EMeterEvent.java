package com.thesis.assignment;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class EMeterEvent {
	private Long houseId;
	private Long timestamp;
	private Double power;

	public EMeterEvent() {
	}

	public EMeterEvent(Long houseId, Long timestamp, Double power) {
		super();
		this.houseId = houseId;
		this.timestamp = timestamp;
		this.power = power;
	}

	public Long getHouseId() {
		return houseId;
	}

	public void setHouseId(Long houseId) {
		this.houseId = houseId;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public Double getPower() {
		return power;
	}

	public void setPower(Double power) {
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
		return "EMeterEvent [houseId=" + houseId + ", timestamp=" + epochToDateTime() + ", power=" + power + "]";
	}

}
