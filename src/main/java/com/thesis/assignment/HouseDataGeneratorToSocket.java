package com.thesis.assignment;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Random;

public class HouseDataGeneratorToSocket {

	public static void main(String[] args) throws IOException, InterruptedException {
		Long counter = 0l;
		Random rand = new Random();
		LocalDateTime baseTime = LocalDateTime.of(2018, Month.JANUARY, 1, 0, 0);

		if (args.length != 3) {
			System.err.println("USAGE:\nHouseDataGeneratorToSocket <port> <no. of events> <unique houses>");
			return;
		}

		Integer port = Integer.parseInt(args[0]);
		Integer limit = Integer.parseInt(args[1]);
		Long houses = Long.parseLong(args[2]);

		System.out.println("starting event generation...");

		try (ServerSocket ss = new ServerSocket(port);
				Socket socket = ss.accept();
				OutputStream outputStream = socket.getOutputStream();
				DataOutputStream dataOutputStream = new DataOutputStream(outputStream);) {

			while (counter < limit) {
				Long id = counter % houses;
				Double power = 10 + rand.nextDouble() * 20;
				if (id == 0) {
					// baseTime = baseTime.plusHours(1);
					baseTime = baseTime.plusMinutes(30);
				}
				Long time = baseTime.toInstant(ZoneOffset.UTC).toEpochMilli();
				EMeterEvent event = new EMeterEvent(id, time, power);
				dataOutputStream.writeUTF(event.toString());
				dataOutputStream.flush();
				// Thread.sleep(10);
				counter++;
			}
			System.out.println("event generation completed...");
		}
	}

}
