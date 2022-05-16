package edu.upenn.cis.crawler.storage;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="visitedURL")
public class visitedURLItem {
	
	private String url;

	@DynamoDBHashKey(attributeName="url")
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public visitedURLItem(){}

}
