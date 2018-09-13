package com.defne.dbconnector;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class MySqlConnector extends SqlConnector {
	// JDBC driver name and database URL
	final static String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	String DB_URL = "jdbc:mysql://";
	
    // Query
    String FIELD_QUERY = "SHOW COLUMNS FROM $tablename";
    
    String SELECT_QUERY = "SELECT * FROM $tablename WHERE $primarykey > ? "
    		+ "ORDER BY $primarykey ASC LIMIT $pagesize";
    
    String SELECT_FIRST_QUERY = "SELECT * "
    							+ "FROM $tablename " 
    							+ "LIMIT 1;";
    							
    String COUNT_QUERY = "SELECT COUNT(*) AS COUNT FROM $tablename";
    
    String QUERY_FOR_PRIMARY_KEY = "SELECT GROUP_CONCAT(COLUMN_NAME) AS PRIMARYKEY, TABLE_NAME "
			+"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=?"
			+" AND CONSTRAINT_NAME=? AND TABLE_NAME=? GROUP BY TABLE_NAME;";
    
    // Information on table
	
	
	// Create singleton
 	private static MySqlConnector mysqlConnector = new MySqlConnector();
   	private MySqlConnector() { }	  	
   	public static MySqlConnector getInstance() {
 	  	  return mysqlConnector;
   	}
    // Connection
	Connection conn;
    ResultSet rs;
    
    public void getDbProperties() throws IOException {
  		Properties configProperties = new Properties();
  		String path = "./config.properties";
  		
  		FileInputStream file = new FileInputStream(path);
  		
  		configProperties.load(file);
  		file.close();
  		
  		String dbname = configProperties.getProperty("sql.dbname");
  		if (dbname.contains(" ")) {
  			throw new IllegalArgumentException();
  		} else {
  			this.DB_NAME = dbname; 
  		}
  		
  		String tableName = configProperties.getProperty("sql.table_name");
  		if (tableName.contains(" ")) {
  			throw new IllegalArgumentException();
  		} else {
  			TABLE_NAME = tableName; 
  		}
  		
  		this.USER = configProperties.getProperty("sql.username");
  		this.PASS = configProperties.getProperty("sql.password");
  		this.DB_URL += configProperties.getProperty("sql.host") + '/' + this.DB_NAME;
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
	    
	    System.out.println("Successfully connected to mysql db, " + this.DB_URL);
    }
    
    
    public PreparedStatement generateQuery() throws SQLException {
    	SELECT_QUERY = SELECT_QUERY.replace("$tablename", TABLE_NAME)
    							   .replace("$primarykey", primaryKey)
    							   .replace("$pagesize", Integer.toString(pageCapacity));	
    	
    	PreparedStatement statement = getConn().prepareStatement(SELECT_QUERY);
    	if (getLatestReadPrimaryKey() == null) {
    		statement.setString(1,  firstRowKey);
    	} else {
    		statement.setString(1,  getLatestReadPrimaryKey());
    	}
    	
    	return statement;
    }
    
    public PreparedStatement generateFirstRowQuery() throws SQLException{
    	SELECT_FIRST_QUERY = SELECT_FIRST_QUERY.replace("$tablename", TABLE_NAME);
    	PreparedStatement statement = getConn().prepareStatement(SELECT_FIRST_QUERY);
    	
    	return statement;
    }
    
    public PreparedStatement generateFieldQuery() throws SQLException {
    	FIELD_QUERY = FIELD_QUERY.replace("$tablename", TABLE_NAME);
    	PreparedStatement statement = getConn().prepareStatement(FIELD_QUERY);
    	
    	return statement;
    }
    
    public PreparedStatement generateCountQuery() throws SQLException {
    	COUNT_QUERY = COUNT_QUERY.replace("$tablename", TABLE_NAME);
    	PreparedStatement statement = getConn().prepareStatement(COUNT_QUERY);
    	
    	return statement;
    }
    
    public PreparedStatement generateQueryForPrimaryKey() throws SQLException {
    	PreparedStatement statement = getConn().prepareStatement(QUERY_FOR_PRIMARY_KEY);
    	statement.setString(1, DB_NAME);
    	statement.setString(2,  "PRIMARY");
    	statement.setString(3, TABLE_NAME);
    	
    	return statement;
    }


}
