package com.defne.dbconnector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MsSqlConnector extends SqlConnector implements GenerateQuery {
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";  
    String DB_URL = "jdbc:sqlserver://";
    
    //Query
    String FIELD_QUERY = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS "
    +"WHERE TABLE_NAME = ?";
    
    String SELECT_QUERY = "SELECT * FROM $tablename WHERE $primarykey > ? "
    		+ "ORDER BY $primarykey ASC LIMIT $pagesize";
   
    String COUNT_QUERY = "SELECT COUNT(*) FROM $tablename";
    
    String QUERY_FOR_PRIMARY_KEY = "SELECT GROUP_CONCAT(COLUMN_NAME), TABLE_NAME "
			+"FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=?"
			+" AND CONSTRAINT_NAME=? AND TABLE_NAME=? GROUP BY TABLE_NAME;";
    
    // Create singleton
 	private static MsSqlConnector msSqlConnector = new MsSqlConnector();
   	private MsSqlConnector() { }	  	
   	public static MsSqlConnector getInstance( ) {
 	  	  return msSqlConnector;
 	}
   	
   	public PreparedStatement generateQuery() throws SQLException {
    	SELECT_QUERY = SELECT_QUERY.replace("$tablename", TABLE_NAME)
    							   .replace("$primarykey", primaryKey)
    							   .replace("$pagesize", Integer.toString(pageCapacity));	
    	
    	PreparedStatement statement = getConn().prepareStatement(SELECT_QUERY);
    	if (getLatestReadPrimaryKey() == null) {
    		statement.setString(1,  "0");
    	} else {
    		statement.setString(1,  getLatestReadPrimaryKey());
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
    	statement.setString(1, DB_NAME);
    	statement.setString(2,  "PRIMARY");
    	statement.setString(3, TABLE_NAME);
    	
    	return statement;
    }
    
}
