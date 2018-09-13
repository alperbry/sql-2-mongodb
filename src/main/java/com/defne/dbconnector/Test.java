package com.defne.dbconnector;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class Test {
	static volatile boolean hasReadDb = false;
	static volatile boolean hasFinishedReadingDb = false;
	static int rowCount;
	static int insertedRowCount;
	final static int pageCapacity = 10000;
	
	static BlockingQueue<Map<String,Object>> queue = new ArrayBlockingQueue<Map<String,Object>>(pageCapacity);

	public static void main(String[] args) {
		Thread producer = new Thread(new FromSql());
		producer.start();
		
		Thread consumer= new Thread(new ToMongo());
		consumer.start();
		
		progressBar();
		
	}
	
	public static void progressBar() {
		
		try {
			Thread.sleep(1500);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		while (true) {
			int total = 50;
			
			int leftCount = rowCount - insertedRowCount;
			int cycleCount = (leftCount / pageCapacity);
			int doneCycleCount = insertedRowCount / pageCapacity;
			
			int doneSignCount = (total * doneCycleCount) / (cycleCount + doneCycleCount);
			
			String done = "=";
			String notYet= " ";
			String print = "";
			for (int i = 0 ; i<doneSignCount; i++) {
				print = print + done;
				
			}
			for (int i = 0 ; i<total - doneSignCount; i++) {
				print = print + notYet;
				
			}
			String doneSoFar = Integer.toString(insertedRowCount) 
								+ '/' + Integer.toString(rowCount);
			
			String toConsole = "\r|" + print + "|" + doneSoFar;
			
			try {
				System.out.write(toConsole.getBytes());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				Thread.sleep(250);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			if (insertedRowCount >= rowCount) {
				doneSoFar = Integer.toString(rowCount) 
						+ '/' + Integer.toString(rowCount);
				toConsole = "\r|" + print + "|" + doneSoFar + " ";
				
				try {
					System.out.write(toConsole.getBytes());
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				break;
			}
		}
		
	}
}
