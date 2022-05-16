package edu.upenn.cis.utils;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "fileToDoc")
public class FileToDoc {

    private String docId;
    private String fileId;

    @DynamoDBHashKey(attributeName = "docid")
    public String getDocId() {return docId;}

    public void setDocId(String docId) {this.docId = docId;}

    @DynamoDBAttribute(attributeName = "fileid")
    public String getFileId() {return fileId;}

    public void setFileId(String fileId) {this.fileId = fileId;}
}
