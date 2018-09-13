package com.defne.dbconnector;

import java.io.IOException;

public class ToMongo implements Runnable{

	public void run() {
		// TODO Auto-generated method stub
		
		while (Test.queue.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		MongodbConnector mongoConnector = MongodbConnector.getInstance();
		try {
			mongoConnector.getDbProperties();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		mongoConnector.createConnection();
		
		System.out.println("Successfully connected to mongodb");
		long startTime = System.currentTimeMillis();
		
		while (true) {
			while (!Test.queue.isEmpty()) {
				mongoConnector.insertDocument(Test.queue.poll());
			}
			
			if (Test.hasFinishedReadingDb) {
				long estimatedTime = System.currentTimeMillis() - startTime;
				System.out.println("\nTime passed: "+ estimatedTime + " ms\n");
				break;
			}
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		mongoConnector.closeConnection();	
	}
}
