package edu.upenn.cis.crawler.storage;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="fileToDoc")
public class fileIdToDocIdItem {

	private String fileId;
	private String docId;
	
	@DynamoDBAttribute(attributeName = "fileid")
	public String getFileId() {
		return fileId;
	}
	public void setFileId(String fileId) {
		this.fileId = fileId;
	}
	
	@DynamoDBHashKey(attributeName="docid")
	public String getDocId() {
		return docId;
	}
	public void setDocId(String docId) {
		this.docId = docId;
	}
	
	public fileIdToDocIdItem() {}

}
