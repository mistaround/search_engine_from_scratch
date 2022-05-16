package edu.upenn.cis.crawler.storage;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="queue")
public class queueItem {

	private String queue;

	@DynamoDBHashKey(attributeName="url")
	public String getQueue() {
		return queue;
	}

	public void setQueue(String queue) {
		this.queue = queue;
	}
	
	public queueItem() {}
	
}
