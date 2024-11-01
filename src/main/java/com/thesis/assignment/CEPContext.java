package com.thesis.assignment;

import java.util.List;

public class CEPContext {

	private List<OperationContext> events;

	public CEPContext() {
	}

	public CEPContext(List<OperationContext> events) {
		super();
		this.events = events;
	}

	public List<OperationContext> getEvents() {
		return events;
	}

	public void setEvents(List<OperationContext> events) {
		this.events = events;
	}

	@Override
	public String toString() {
		return "CEPContext [events=" + events + "]";
	}

}
