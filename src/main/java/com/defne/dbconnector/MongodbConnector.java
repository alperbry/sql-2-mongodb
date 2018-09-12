package com.defne.dbconnector;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongodbConnector {
	String hostName = "";
	String dbName = "";
	String collectionName = "";
	
	MongoClient mongo;
	MongoDatabase database;
	MongoCollection<Document> collection;
	
	private int insertCount;

	//Create singleton
	private static MongodbConnector mongoDbConnector = new MongodbConnector();
	private MongodbConnector() { }
	public static MongodbConnector getInstance( ) {
	      return mongoDbConnector;
	}
	
	public void getDbProperties() throws IOException {
  		Properties configProperties = new Properties();
  		String path = "/Users/mac/dbtestworkspace/dbconnector/resources/config.properties";
  		
  		FileInputStream file = new FileInputStream(path);
  		
  		configProperties.load(file);
  		file.close();
  		
  		String dbname = configProperties.getProperty("mongo.dbname");
  		if (dbname.contains(" ")) {
  			throw new IllegalArgumentException();
  		} else {
  			this.dbName = dbname; 
  		}
  		
  		String tableName = configProperties.getProperty("mongo.collection_name");
  		if (tableName.contains(" ")) {
  			throw new IllegalArgumentException();
  		} else {
  			this.collectionName = tableName; 
  		}
  		
   		this.hostName = configProperties.getProperty("mongo.host");
  	}
	
	
	public void createConnection() {
		try {
			mongo = new MongoClient();
		    database = mongo.getDatabase(dbName);
		    collection = database.getCollection(collectionName);
		} catch (Exception e) {
			
		}	
	}
	
	public void closeConnection() {
		mongo.close();
	}
	
	public void insertDocument(Map<String, Object> row) {
		if (row.isEmpty() || row == null) {
			return;
		}
		
		Document document = new Document(row);
		collection.insertOne(document);
		increaseInsertCount();
	}
	
	public void increaseInsertCount() {
		insertCount++;
		Test.insertedRowCount = insertCount;
	}

}
