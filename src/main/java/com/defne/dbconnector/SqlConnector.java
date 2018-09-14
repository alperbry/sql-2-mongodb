package com.defne.dbconnector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public abstract class SqlConnector {
	// JDBC driver name and database URL
	String JDBC_DRIVER = "";  
    String DB_URL = "";
    String DB_NAME = "";
    String TABLE_NAME = "";
    
    //  Database credentials
    String USER = "";
    String PASS = "";
	
    
    // Information on table
    static List<String> fieldNames = new ArrayList<String>();
    static Map<String,Object> tableMap;
    private int rowCount;
	private int readCount;
	
	private String latestReadPrimaryKey;
    static final int pageCapacity = 10000;
    static String primaryKey;
    static String firstRowValueOfKey;
    
    // Connection
    protected static Connection conn;
    protected ResultSet rs;
  	
  	
  	public static Connection getConn() {
		return conn;
	}
  	
	public static void setConn(Connection conn) {
		SqlConnector.conn = conn;
	}
	
	public String getLatestReadPrimaryKey() {
		return latestReadPrimaryKey;
	}


	public void setLatestReadPrimaryKey(String latestReadPrimaryKey) {
		this.latestReadPrimaryKey = latestReadPrimaryKey;
	}

	
    public ResultSet execute(PreparedStatement statement) {
    	ResultSet result = null;
    	
    	try {
			result = statement.executeQuery();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return result;
    }
    
    public void getRowCountOfTable() {
    	try {
			while(rs.next()){
			     //Retrieve by column name
			     int count = rs.getInt("COUNT");

			     rowCount = count;
			     Test.rowCount = rowCount;
			  }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void getFirstRowKey() {
    	try {	
			//Retrieve by column name
    		rs.next();
    		String firstRowValueOfKey = rs.getString(primaryKey);
    		SqlConnector.firstRowValueOfKey = firstRowValueOfKey;
    		rs.previous();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void getFields() {
    	try {
			while(rs.next()){
			     //Retrieve by column name
			     String field = rs.getString("Field");
			     fieldNames.add(field);
			  }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public boolean getPrimaryKey() {
    	try {
			while(rs.next()){
			     primaryKey = rs.getString("PRIMARYKEY");
			  }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	if (primaryKey == null || primaryKey.isEmpty()) {
    		return false;
    	} else {
    		return true;
    	}
    }
	
    public void extract(ResultSet rs, BlockingQueue<Map<String, Object>> queue) throws InterruptedException {
    	
    	try {
			while(rs.next()){
				tableMap = new HashMap<String, Object>();
				
				for (String fieldName:fieldNames) {
					String fieldData = rs.getString(fieldName);
					tableMap.put(fieldName, fieldData);
					
					if (fieldName.equals(primaryKey)) {
						setLatestReadPrimaryKey(fieldData);
					}
				}
				if (!tableMap.isEmpty()) {
					queue.put(tableMap);
				}
				
				increaseReadCount();
			 }
		
			if (rs.last()) {
				for (String fieldName:fieldNames) {
					String fieldData = rs.getString(fieldName);
					tableMap.put(fieldName, fieldData);
					
					if (fieldName.equals(primaryKey)) {
						setLatestReadPrimaryKey(fieldData);
					}
				}
			}
			
			tableMap = null;
			rs = null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    
    public void increaseReadCount() {
    	readCount += 1;
    	
    	if (readCount > rowCount) {
    		readCount = rowCount;
    	}
    }
    
    public int getReadCount() {
		return readCount;
	}
    
    public void setReadCount(int readCount) {
		this.readCount = readCount;
	}
    
	public int getRowCount() {
		return rowCount;
	}
	
	public void progressBar() {
		int total = 50;
		
		int leftCount = rowCount - readCount;
		int cycleCount = (leftCount / pageCapacity);
		int doneCycleCount = readCount / pageCapacity;
		
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
		String doneSoFar = Integer.toString(readCount) 
							+ '/' + Integer.toString(rowCount);
		
		String toConsole = "\r|" + print + "|" + doneSoFar;
		
		try {
			System.out.write(toConsole.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void closeConnection() {
    	try {
			rs.close();
			getConn().close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public abstract PreparedStatement generateQueryForPrimaryKey() throws SQLException;
	public abstract PreparedStatement generateQuery() throws SQLException;
	public abstract PreparedStatement generateFieldQuery() throws SQLException;
	public abstract PreparedStatement generateCountQuery() throws SQLException;
	public abstract PreparedStatement generateFirstRowQuery() throws SQLException;
	public abstract void createConnection() throws ClassNotFoundException, SQLException;
	public abstract void getDbProperties() throws IOException;
}
