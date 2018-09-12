package com.defne.dbconnector;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface GenerateQuery {
	public PreparedStatement generateQuery() throws SQLException;
	public PreparedStatement generateFieldQuery() throws SQLException;
	public PreparedStatement generateCountQuery() throws SQLException;
	public PreparedStatement generateQueryForPrimaryKey() throws SQLException;
}
