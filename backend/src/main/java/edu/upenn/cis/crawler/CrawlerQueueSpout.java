package edu.upenn.cis.crawler;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

import edu.upenn.cis.crawler.utils.URLInfo;
import edu.upenn.cis.crawler.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.crawler.stormlite.TopologyContext;
import edu.upenn.cis.crawler.stormlite.routers.IStreamRouter;
import edu.upenn.cis.crawler.stormlite.spout.IRichSpout;
import edu.upenn.cis.crawler.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.crawler.stormlite.tuple.Fields;
import edu.upenn.cis.crawler.stormlite.tuple.Values;

public class CrawlerQueueSpout implements IRichSpout{

	static Logger log = LogManager.getLogger(CrawlerQueueSpout.class);

    String executorId = UUID.randomUUID().toString();

    SpoutOutputCollector collector;
    AmazonSQS sqs;
    
    public CrawlerQueueSpout() {
    	log.debug("Starting spout");
    }
    
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("url","domain"));
	}

	@Override
	public void open(Map<String, String> config, TopologyContext topo, SpoutOutputCollector collector) {
		this.sqs = Crawler.getItem().sqs;
		this.collector = collector;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}

	@Override
	public void nextTuple() {
		String queueUrl = Crawler.getItem().fifoQueueUrl;
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
				  .withWaitTimeSeconds(0)
				  .withMaxNumberOfMessages(1);
		List<Message> sqsMessages = sqs.receiveMessage(receiveMessageRequest).getMessages();
		if(sqsMessages.size()>0) {
			Crawler.getItem().setWorking(true);
			Message message = sqsMessages.get(0);
			String url = message.getBody();
			sqs.deleteMessage(new DeleteMessageRequest()
					  .withQueueUrl(queueUrl)
					  .withReceiptHandle(sqsMessages.get(0).getReceiptHandle()));
			URLInfo sentUrl = new URLInfo(url);
			String domain = sentUrl.getHostName()==null? "unknown" : sentUrl.getHostName();
			this.collector.emit(new Values<Object>(url, domain));
			Crawler.getItem().setWorking(false);
		}
//		Thread.yield();
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

}
