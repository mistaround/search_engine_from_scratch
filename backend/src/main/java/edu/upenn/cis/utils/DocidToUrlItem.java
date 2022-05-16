package edu.upenn.cis.utils;

import java.util.UUID;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName="docid_to_url")
public class DocidToUrlItem {

    private String url;
    private String docid;

    @DynamoDBAttribute(attributeName="url")
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }


    @DynamoDBHashKey(attributeName="docid")
    public String getDocid() {
        return docid;
    }
    public void setDocid(String docid) {
        this.docid = docid;
    }

    public DocidToUrlItem () {}
}
