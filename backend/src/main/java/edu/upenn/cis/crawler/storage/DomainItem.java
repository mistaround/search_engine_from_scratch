package edu.upenn.cis.crawler.storage;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="domain")
public class DomainItem {
	
	private String domainName;
	private long lastAccessTime = 0;
	private int delay = 0;
	
	@DynamoDBHashKey(attributeName="domainName")
	public String getDomainName() {
		return domainName;
	}
	public void setDomainName(String domainName) {
		this.domainName = domainName;
	}
	
	@DynamoDBAttribute(attributeName = "lastAccessTime")
	public long getLastAccessTime() {
		return lastAccessTime;
	}
	public void setLastAccessTime(long lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}
	
	@DynamoDBAttribute(attributeName = "delay")
	public int getDelay() {
		return delay;
	}
	public void setDelay(int delay) {
		this.delay = delay;
	}
	
	public DomainItem(){}

}
