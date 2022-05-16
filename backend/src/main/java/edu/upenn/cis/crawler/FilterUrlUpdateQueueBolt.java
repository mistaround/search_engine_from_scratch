package edu.upenn.cis.crawler;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import edu.upenn.cis.crawler.storage.Storage;
import edu.upenn.cis.crawler.storage.visitedURLItem;
import edu.upenn.cis.crawler.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.crawler.stormlite.TopologyContext;
import edu.upenn.cis.crawler.stormlite.bolt.IRichBolt;
import edu.upenn.cis.crawler.stormlite.bolt.OutputCollector;
import edu.upenn.cis.crawler.stormlite.routers.IStreamRouter;
import edu.upenn.cis.crawler.stormlite.tuple.Fields;
import edu.upenn.cis.crawler.stormlite.tuple.Tuple;

public class FilterUrlUpdateQueueBolt implements IRichBolt{
	static Logger logger = LogManager.getLogger(FilterUrlUpdateQueueBolt.class);

	Fields schema = new Fields("filteredUrl"); 
	
    String executorId = UUID.randomUUID().toString();

    DynamoDBMapper mapper;
    
    private OutputCollector collector;
//    domainAccessTable domainTable = new domainAccessTable(db);

    public FilterUrlUpdateQueueBolt() {
    }
    
	@Override
	public String getExecutorId() {
		return executorId;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		Crawler.getItem().setWorking(true);
		String url = input.getStringByField("extractedUrl");
		List <SendMessageBatchRequestEntry> messageEntries = new ArrayList<>();
		if(urlFilter(url)) {
			SendMessageRequest send_msg_request = new SendMessageRequest()
	    	        .withQueueUrl(Crawler.getItem().fifoQueueUrl)
	    	        .withMessageBody(url)
	    	        .withMessageGroupId("baeldung-group-1");
	    	Crawler.getItem().sqs.sendMessage(send_msg_request);
		}
		Crawler.getItem().setWorking(false);
		
	}

	@Override
	public void prepare(Map<String, String> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		mapper = new DynamoDBMapper(((Storage)Crawler.getItem().db).dynamoDB);
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);		
	}

	@Override
	public Fields getSchema() {
		return schema;
	}
	
	public boolean urlFilter(String url) {
        try {
            new URL(url).toURI();
            visitedURLItem item = mapper.load(visitedURLItem.class, url);
            if(item==null) {
            	return true;
            }
            return false;
        }
        catch (Exception e) {
            return false;
        }
	}
	

}
