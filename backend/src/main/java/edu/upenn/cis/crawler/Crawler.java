package edu.upenn.cis.crawler;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Level;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import edu.upenn.cis.crawler.storage.StorageFactory;
import edu.upenn.cis.crawler.storage.StorageInterface;

public class Crawler implements CrawlMaster {
    ///// TODO: you'll need to flesh all of this out. You'll need to build a thread
    // pool of CrawlerWorkers etc.

    static final int NUM_WORKERS = 16;
    String startUrl = null;
    public StorageInterface db = null;
    int maxSize = 0;
    int count = 0; 
    public AmazonSQS sqs;
    public String fifoQueueUrl;
    public String queue;
    public static String fileId = UUID.randomUUID().toString();
    private static AtomicLong numOfFile = new AtomicLong(0);
    public ConcurrentHashMap<Thread, String> workerStatus = new ConcurrentHashMap<>();
    public ConcurrentHashMap<Thread, String> getWorkerStatus() {
		return workerStatus;
	}

	public void setWorkerStatus(ConcurrentHashMap<Thread, String> workerStatus) {
		this.workerStatus = workerStatus;
	}

	public AtomicBoolean isDBClosed = new AtomicBoolean(false);
    
    private static Crawler instance = null;
    
    public Crawler(String startUrl, StorageInterface db, int size, int count) {
        // TODO: initialize
    	this.startUrl = startUrl;
    	this.db = db;
    	this.maxSize = size;
    	this.count = count;
    	instance = this;
    }
    
    public static Crawler getItem()
    {
        return instance;
    }

    /**
     * Main thread
     * @throws Exception 
     */
    public void start() throws Exception {
    	//build a queue and add start url into the queue
    	sqs = initUrlQueue();
    	Map<String, String> queueAttributes = new HashMap<>();
		queueAttributes.put("FifoQueue", "true");
		queueAttributes.put("ContentBasedDeduplication", "true");
		queueAttributes.put("VisibilityTimeout", "1");
		CreateQueueRequest createFifoQueueRequest = new CreateQueueRequest(
				credentialSet.queue).withAttributes(queueAttributes);
		fifoQueueUrl = sqs.createQueue(createFifoQueueRequest).getQueueUrl();
		
		SendMessageRequest sendMessageFifoQueue = new SendMessageRequest()
				  .withQueueUrl(fifoQueueUrl)
				  .withMessageBody(startUrl)
				  .withMessageGroupId("baeldung-group-1");
		sqs.sendMessage(sendMessageFifoQueue);
		for(int i=0;i<NUM_WORKERS;i++) {
			CrawlerTask.build();
		}
    }
    
    private AmazonSQS initUrlQueue() {
    	System.out.println(credentialSet.AccessKeyID);
    	AWSCredentials credentials = new BasicAWSCredentials(
    			credentialSet.AccessKeyID, 
    			credentialSet.SecretAccessKey
  			);
    	
    	AmazonSQS sqs = AmazonSQSClientBuilder.standard()
    			  .withCredentials(new AWSStaticCredentialsProvider(credentials))
    			  .withRegion(Regions.US_EAST_1)
    			  .build();
    	return sqs;
    	
    	
    }
    /**
     * We've indexed another document
     */
    @Override
    public void incCount() {
    	numOfFile.getAndIncrement();
    }
    
    public long getNumOfFile() {
    	return numOfFile.get();
    }

    /**
     * Workers can poll this to see if they should exit, ie the crawl is done
     */
    @Override
    public boolean isDone() {
//    	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(fifoQueueUrl)
//				  .withWaitTimeSeconds(0)
//				  .withMaxNumberOfMessages(1);
//		List<Message> sqsMessages = sqs.receiveMessage(receiveMessageRequest).getMessages();
//    	//if over limits
    	if(getNumOfFile()>=count) {
    		return true;
////    		//queue is empty
//    	}else if(workerStatus.size()>0 && !workerStatus.containsValue("working") && sqsMessages.size()==0) {
//    		return true;
    	}
        return false;
    }

    /**
     * Workers should notify when they are processing an URL
     */
    @Override
    public void setWorking(boolean working) {
    	if(working) {
    		workerStatus.put(Thread.currentThread(), "working");
    	}else {
    		workerStatus.put(Thread.currentThread(), "waiting");
    	}
    }

    /**
     * Workers should call this when they exit, so the master knows when it can shut
     * down
     */
    @Override
    public void notifyThreadExited() {
    	workerStatus.put(Thread.currentThread(), "exited");
    }

    /**
     * Main program: init database, start crawler, wait for it to notify that it is
     * done, then close.
     * @throws Exception 
     */
    public static void main(String args[]) throws Exception {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.INFO);
        if (args.length < 3 || args.length > 5) {
            System.out.println("Usage: Crawler {start URL} {max doc size in MB} {number of files to index}  {name of queue}");
            System.exit(1);
        }

        System.out.println("Crawler starting"); 
        String startUrl = args[0];
        Integer size = Integer.valueOf(args[1]);
        Integer count = args.length == 4 ? Integer.valueOf(args[2]) : 100;
        credentialSet.queue = args[3];
        System.out.println("queue is "+credentialSet.queue);
        
        StorageInterface db = StorageFactory.getDatabaseInstance();
        
        
        Crawler crawler = new Crawler(startUrl, db, size, count);
        

        System.out.println("Starting crawl of " + count + " documents, starting at " + startUrl);
        crawler.start();

        while (!crawler.isDone()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } 
        
        // TODO: final shutdown
        System.out.println("shutdown");
        
        try {
        	Thread.sleep(60000);
        	crawler.isDBClosed.set(true);
            db.close();
            System.out.println("Done crawling!"+crawler.getNumOfFile());
            System.exit(0);
        } catch (Exception e) {
            // TODO Auto-generated catch block
        	
        } finally {
        	System.out.println("Done!");
        	System.exit(0);
        }
    }

}
