package edu.upenn.cis.crawler.storage;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAutoGeneratedKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="checksum")
public class ChecksumItem {

//	private String id;
	private String hash;
	
//	@DynamoDBHashKey(attributeName="id")
//	@DynamoDBAutoGeneratedKey
//	public String getId() { return id; }
//    public void setId(String id) {this.id = id; }
    
    @DynamoDBHashKey(attributeName="hash")
    public String getHash() {return hash; }
    public void setHash(String hash) { this.hash = hash; }
    
    public ChecksumItem() {}

}
