package edu.upenn.cis.crawler.storage;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import edu.upenn.cis.crawler.credentialSet;


public class StorageFactory {
    public static StorageInterface getDatabaseInstance() {
        // TODO: factory object, instantiate your storage server    	
    	AWSCredentials credentials = new BasicAWSCredentials(
    			credentialSet.AccessKeyID, 
    			credentialSet.SecretAccessKey
    			);
    	AmazonS3 s3client = AmazonS3ClientBuilder
    			  .standard()
    			  .withCredentials(new AWSStaticCredentialsProvider(credentials))
    			  .withRegion(Regions.US_EAST_1)
    			  .build();
    	AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.build();

        Storage storage = new Storage(s3client, dynamoDB);
        return storage;
    }
}
