package com.defne.dbconnector;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class FromSql implements Runnable{
	static SqlConnector sqlConnector;
	
	public void run() {
		// TODO Auto-generated method stub	
		sqlConnector = MySqlConnector.getInstance();
		
		configureConnection();
			
		while (sqlConnector.getReadCount() < sqlConnector.getRowCount()) {
			getPageFromDb(sqlConnector);
		}
		
		Test.hasFinishedReadingDb = true;
		sqlConnector.closeConnection();
	}
	
	public static void readProperties() {
		try {
			sqlConnector.getDbProperties();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public static void getPrimaryKeyOfTable() {
		try {
			PreparedStatement statement = null;
			statement = sqlConnector.generateQueryForPrimaryKey();
			sqlConnector.rs = sqlConnector.execute(statement);
			sqlConnector.getPrimaryKey();
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	}
	
	public static void getRowCountOfTable() {
		try {
			PreparedStatement statement = null;
			statement = sqlConnector.generateCountQuery();
			sqlConnector.rs = sqlConnector.execute(statement);
			sqlConnector.getRowCountOfTable();
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	}
	
	public static void getFieldsOfTable() {
		try {
			PreparedStatement statement = null;
			statement = sqlConnector.generateFieldQuery();
			sqlConnector.rs = sqlConnector.execute(statement);
			sqlConnector.getFields();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public static void getFirstRow() {
		try {
			PreparedStatement statement = null;
			statement = sqlConnector.generateFirstRowQuery();
			sqlConnector.rs = sqlConnector.execute(statement);
			sqlConnector.getFirstRowKey();
			try {
				sqlConnector.extract(sqlConnector.rs, Test.queue);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
	}
	
	public static void getPageFromDb(SqlConnector sqlConnector) {
		
		PreparedStatement statement = null;
		
		try {
			statement = sqlConnector.generateQuery();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		long startTime = System.currentTimeMillis();
		
		sqlConnector.rs = sqlConnector.execute(statement);
		
		long estimatedTime = System.currentTimeMillis() - startTime;
		StringBuilder time = new StringBuilder();
		time.append("Reading this page from db took ")
				.append(estimatedTime)
				.append(" ms\r");
		System.out.println(time.toString());
	
		startTime = System.currentTimeMillis();
		try {
			sqlConnector.extract(sqlConnector.rs, Test.queue);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		estimatedTime = System.currentTimeMillis() - startTime;
		time = new StringBuilder();
		time.append("Extracting this page from db took ")
				.append(estimatedTime)
				.append(" ms\r");
		
		System.out.println(time.toString());		
	}
	
	public static void configureConnection() {
		readProperties();
		sqlConnector.createConnection();
		
		getPrimaryKeyOfTable();
		getRowCountOfTable();
		getFieldsOfTable();
		getFirstRow();
	}
}
