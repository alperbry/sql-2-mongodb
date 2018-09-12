package com.defne.dbconnector;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public abstract class SqlConnector {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
    String DB_URL = "jdbc:mysql://";
    String DB_NAME = "";
    String TABLE_NAME = "";
    
    //  Database credentials
    static String USER = "";
    static String PASS = "";
	
    // Query
    String FIELD_QUERY = "SHOW COLUMNS FROM $tablename";
    
    String SELECT_QUERY = "SELECT * FROM $tablename WHERE $primarykey > ? "
    		+ "ORDER BY $primarykey ASC LIMIT $pagesize";
   
    String COUNT_QUERY = "SELECT COUNT(*) FROM $tablename";
    
    String QUERY_FOR_PRIMARY_KEY = "SELECT GROUP_CONCAT(COLUMN_NAME), TABLE_NAME "
			+"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=?"
			+" AND CONSTRAINT_NAME=? AND TABLE_NAME=? GROUP BY TABLE_NAME;";
    
    // Information on table
    static List<String> fieldNames = new ArrayList<String>();
    static Map<String,Object> tableMap;
    private int rowCount;
	private int readCount;
	private String latestReadPrimaryKey;
    static final int pageCapacity = 10000;
    static String primaryKey = "";
    
    // Connection
    private static Connection conn;
    public ResultSet rs;
  	
  	
  	public static Connection getConn() {
		return conn;
	}


	public String getLatestReadPrimaryKey() {
		return latestReadPrimaryKey;
	}


	public void setLatestReadPrimaryKey(String latestReadPrimaryKey) {
		this.latestReadPrimaryKey = latestReadPrimaryKey;
	}


	public static void setConn(Connection conn) {
		SqlConnector.conn = conn;
	}


	public void getDbProperties() throws IOException {
  		Properties configProperties = new Properties();
  		String path = "/Users/mac/dbtestworkspace/dbconnector/resources/config.properties";
  		
  		FileInputStream file = new FileInputStream(path);
  		
  		configProperties.load(file);
  		file.close();
  		
  		String dbname = configProperties.getProperty("sql.dbname");
  		if (dbname.contains(" ")) {
  			throw new IllegalArgumentException();
  		} else {
  			DB_NAME = dbname; 
  		}
  		
  		String tableName = configProperties.getProperty("sql.table_name");
  		if (tableName.contains(" ")) {
  			throw new IllegalArgumentException();
  		} else {
  			TABLE_NAME = tableName; 
  		}
  		
  		USER = configProperties.getProperty("sql.username");
  		PASS = configProperties.getProperty("sql.password");
  		DB_URL += configProperties.getProperty("sql.host") + '/' + DB_NAME;
  	}
    
    
    public void createConnection() {
    	try {
			Class.forName(JDBC_DRIVER);
		} catch (ClassNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
    	
    	System.out.println("Connecting to database...");
    	
	    try {
			setConn(DriverManager.getConnection(DB_URL,USER,PASS));
		} catch (SQLException e) {
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
			     //Display values
			     rowCount = count;
			     Test.rowCount = rowCount;
			  }
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
			     //Display values
			     fieldNames.add(field);
			  }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void getPrimaryKey() {
    	try {
			while(rs.next()){
			     primaryKey = rs.getString("GROUP_CONCAT(COLUMN_NAME)");
			  }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
    public void extract(ResultSet rs, BlockingQueue<Map<String, Object>> queue) throws InterruptedException {
    	
    	try {
			while(rs.next()){
				tableMap = new HashMap<String, Object>();
				
				for (String fieldName:fieldNames) {
					String fieldData = rs.getString(fieldName);
					tableMap.put(fieldName, fieldData);
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


}
