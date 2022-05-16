package edu.upenn.cis.crawler.storage;

import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;

import edu.upenn.cis.crawler.credentialSet;
import spark.HaltException;


public class Storage implements StorageInterface{
	
	public static AmazonS3 s3client;
	public static AmazonDynamoDB dynamoDB;
	
	public Storage(AmazonS3 s3client, AmazonDynamoDB dynamoDB) {
		this.s3client = s3client;
		this.dynamoDB = dynamoDB;
		createDocumentBucket();

	}
	
	private void createDocumentBucket() {
		String documentBucket = credentialSet.documentBucket;
		List<Bucket> buckets = s3client.listBuckets();
		for(Bucket bucket : buckets) {
		    System.out.println(bucket.getName());
		}
		if(s3client.doesBucketExist(documentBucket)) {
		    System.out.println("Bucket name is not available."
		      + " Try again with a different Bucket name.");
		    return;
		}
		s3client.createBucket(documentBucket);
	}
	
	@Override
	public int getCorpusSize() {
		return 0;
	}

	@Override
	public int addDocument(String url, String documentContents) {

		return 0;
	}

	@Override
	public String getDocument(String url) {

		return null;
	}

	@Override
	public int addUser(String username, String password) throws HaltException{
		return 0; 
	}

	@Override
	public boolean getSessionForUser(String username, String password) throws HaltException {
		return false;
	}

	@Override
	public void close() {
	}
	
}