package com.defne.dbconnector;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class MsSqlConnector extends SqlConnector {
	// JDBC driver name and database URL
	String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";  
    String DB_URL = "jdbc:sqlserver://";
    
    //Query
    String FIELD_QUERY = "SELECT COLUMN_NAME AS COLUMNS " 
    		+ "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?;";
    
    String SELECT_QUERY = "SELECT * FROM $tablename ORDER BY $orderfield "
    		+ "OFFSET $readcount ROWS FETCH NEXT $pagesize ROWS ONLY;";
    
    String OPTIMIZED_SELECT_QUERY = "SELECT TOP $pagesize *" + 
    		"FROM $tablename " + 
    		"WHERE $primarykey >= ?" + 
    		"ORDER BY $primarykey";
    
    String SELECT_FIRST_QUERY = "SELECT TOP 1 * " + 
    							"FROM $tablename " + 
    							"ORDER BY $primarykey;";
   
    String COUNT_QUERY = "SELECT COUNT(*) FROM $tablename";
    
    String QUERY_FOR_PRIMARY_KEY = "SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEY " + 
    		"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " + 
    		"INNER JOIN " + 
    		"INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " + 
    		"ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND " + 
    		"TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND " + 
    		"KU.table_name=? " + 
    		"ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;";
    
    // Create singleton
 	private static MsSqlConnector msSqlConnector = new MsSqlConnector();
   	private MsSqlConnector() { }	  	
   	public static MsSqlConnector getInstance( ) {
 	  	  return msSqlConnector;
 	}
   	
   	public PreparedStatement generateQuery() throws SQLException {
   		PreparedStatement statement = null;
    	
    	////
    	//If the sql table has primary key
    	//optimized query can be made in the else case
    	////
   		
   		if (primaryKey == null || primaryKey.isEmpty()) {
   			SELECT_QUERY = SELECT_QUERY.replace("$tablename", TABLE_NAME)
					   .replace("$orderfield", fieldNames.get(0))
					   .replace("$pagesize", Integer.toString(pageCapacity));	
   			
   			statement = getConn().prepareStatement(SELECT_QUERY);
   			
   		} else {
   			SELECT_QUERY = SELECT_QUERY.replace("$tablename", TABLE_NAME)
   						.replace("$pagesize", Integer.toString(pageCapacity))
   						.replace("$primarykey", primaryKey);
   			
   			statement = getConn().prepareStatement(SELECT_QUERY);
   			
   			if (getLatestReadPrimaryKey() == null) {
				statement.setString(1,  firstRowValueOfKey);
			} else {
				statement.setString(1,  getLatestReadPrimaryKey());
			}
   		}
   		
    	return statement;
    }
    
    public PreparedStatement generateFieldQuery() throws SQLException {
    	PreparedStatement statement = getConn().prepareStatement(FIELD_QUERY);
    	statement.setString(1, 	TABLE_NAME);
    	
    	return statement;
    }
    
    public PreparedStatement generateCountQuery() throws SQLException {
    	COUNT_QUERY = COUNT_QUERY.replace("$tablename", TABLE_NAME);
    	PreparedStatement statement = getConn().prepareStatement(COUNT_QUERY);
    	
    	return statement;
    }
    
    public PreparedStatement generateQueryForPrimaryKey() throws SQLException {
    	PreparedStatement statement = getConn().prepareStatement(QUERY_FOR_PRIMARY_KEY);
    	statement.setString(1, TABLE_NAME);
    	
    	return statement;
    }
    
    public PreparedStatement generateFirstRowQuery() throws SQLException{
    	SELECT_FIRST_QUERY = SELECT_FIRST_QUERY.replace("$tablename", TABLE_NAME);
    	SELECT_FIRST_QUERY = SELECT_FIRST_QUERY.replace("$primarykey", primaryKey);
    	PreparedStatement statement = getConn().prepareStatement(SELECT_FIRST_QUERY);
    	
    	return statement;
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
    
}
