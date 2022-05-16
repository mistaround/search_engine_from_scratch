package edu.upenn.cis.PageRank;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

public class PageRankHelper {

    @DynamoDBTable(tableName = "PageRank")
    public static class PageRankItem {
        private String docId;
        private double pagerank;

        public PageRankItem(){}

        @DynamoDBHashKey(attributeName = "docId")
        public String getDocId() {return docId;}

        public void setDocId(String docId) {this.docId = docId;}

        @DynamoDBAttribute(attributeName = "pagerank")
        public double getPagerank() {return pagerank;}

        public void setPagerank(double pagerank) {this.pagerank = pagerank;}
    }
}
