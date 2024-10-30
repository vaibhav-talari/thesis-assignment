package com.thesis.assignment;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class DataGenerator {

	public static void main(String[] args) throws IOException, InterruptedException {
		try (ServerSocket ss = new ServerSocket(1234);
				Socket socket = ss.accept();
				OutputStream outputStream = socket.getOutputStream();
				DataOutputStream dataOutputStream = new DataOutputStream(outputStream);) {
			System.out.println("ServerSocket awaiting connections...");

			EnergyModel em = new EnergyModel();

			int count = 0;
			while (count < 100000) {

				dataOutputStream.writeUTF(em.generateData());
				dataOutputStream.flush();
				// Thread.sleep(10);
				count++;
			}
			System.out.println("Closing socket and terminating program.");
		}
	}

}
