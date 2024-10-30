package com.thesis.assignment;

import java.io.OutputStream;
import java.time.LocalDateTime;
import java.time.Month;
import java.util.Random;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.DataOutputStream;
//import java.util.stream.Stream;

public class Gen {

	public static void main(String[] args) throws IOException, InterruptedException{
		System.out.println("Hi.");
		ServerSocket ss = new ServerSocket(1234);
		System.out.println("ServerSocket awaiting connections...");
		Socket socket = ss.accept();
		DataPoint tt = new DataPoint();
		OutputStream outputStream = socket.getOutputStream();
		DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
		int count =0;
		while(count < 10000){
			for (int i = 0; i < 10; i++) {
				dataOutputStream.writeUTF(tt.generateData());
				dataOutputStream.flush();
			}
			Thread.sleep(10);
			count++;
		}
		dataOutputStream.close(); // close the output stream when we're done.

		System.out.println("Closing socket and terminating program.");
		socket.close();
		//Stream<String> streamGenerated = Stream.generate(tt::generateData);
		//streamGenerated.limit(100000).forEach(System.out::println);
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
