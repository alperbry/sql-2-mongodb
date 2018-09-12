package com.defne.dbconnector;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class FromSql implements Runnable{

	public void run() {
		// TODO Auto-generated method stub
		
		MysqlConnector sqlConnector = MysqlConnector.getInstance();
		
		//
		//Read properties file 
		//
		try {
			sqlConnector.getDbProperties();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		sqlConnector.createConnection();
		
		System.out.println("Successfully connected to mysql db, " + sqlConnector.DB_URL);
		
		PreparedStatement statement = null;
		
		
		try {
			statement = sqlConnector.generateQueryForPrimaryKey();
			sqlConnector.rs = sqlConnector.execute(statement);
			sqlConnector.getPrimaryKey();
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		
		try {
			statement = sqlConnector.generateCountQuery();
			sqlConnector.rs = sqlConnector.execute(statement);
			sqlConnector.getRowCountOfTable();
		} catch (SQLException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		try {
			statement = sqlConnector.generateFieldQuery();
			sqlConnector.rs = sqlConnector.execute(statement);
			sqlConnector.getFields();
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				
		
		while (sqlConnector.getReadCount() < sqlConnector.getRowCount()) {
			try {
				statement = sqlConnector.generateQuery();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			long startTime = System.currentTimeMillis();
			
			//Test.hasReadDb = false;
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
			
			//sqlConnector.progressBar();
			
			/*try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/
		}
		Test.hasFinishedReadingDb = true;
		sqlConnector.closeConnection();
	}
}
