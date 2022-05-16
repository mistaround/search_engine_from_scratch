package edu.upenn.cis.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import edu.upenn.cis.crawler.storage.DomainItem;
import edu.upenn.cis.crawler.storage.Storage;
import edu.upenn.cis.crawler.storage.visitedURLItem;
import edu.upenn.cis.crawler.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.crawler.stormlite.TopologyContext;
import edu.upenn.cis.crawler.stormlite.bolt.IRichBolt;
import edu.upenn.cis.crawler.stormlite.bolt.OutputCollector;
import edu.upenn.cis.crawler.stormlite.routers.IStreamRouter;
import edu.upenn.cis.crawler.stormlite.tuple.Fields;
import edu.upenn.cis.crawler.stormlite.tuple.Tuple;
import edu.upenn.cis.crawler.stormlite.tuple.Values;

public class DocumentFetcherBolt implements IRichBolt{
	static Logger logger = LogManager.getLogger(DocumentFetcherBolt.class);

	Fields schema = new Fields("url", "document","modifiedTime", "type"); 
	
    String executorId = UUID.randomUUID().toString();
    String type = null;
	String userAgent = null;
	String hostname = null;
	long modifiedTime = 0;
	DynamoDBMapper mapper;

    private OutputCollector collector;

    public DocumentFetcherBolt() {}
    
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
		String url = input.getStringByField("url");
		hostname =  input.getStringByField("domain");
		try {
			 
			String result = processURL(url);
			logger.debug(getExecutorId() + " received " + url);
			if(result!=null) {
				collector.emit(new Values<Object>(url, result, modifiedTime, type));
			}
		}catch(Exception e) {
			e.printStackTrace();
			System.err.println("DataBase is closed");
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
	
	public static String getContent(String url) {
		try {
			URL urlObj = new URL(url);
			HttpURLConnection httpCon = null;
			HttpsURLConnection httpsCon = null;
			String result = null;
			if(url.startsWith("https")) {
				httpsCon = (HttpsURLConnection) urlObj.openConnection();
				httpsCon.setRequestMethod("GET");
				httpsCon.setRequestProperty("User-Agent", "cis455crawler");
				httpsCon.connect();
			}else {
				httpCon = (HttpURLConnection) urlObj.openConnection();
				httpCon.setRequestMethod("GET");
				httpCon.setRequestProperty("User-Agent", "cis455crawler");
				httpCon.connect();
			}
			
			int responseCode = httpCon!=null ? httpCon.getResponseCode(): httpsCon.getResponseCode();
			if (responseCode != HttpURLConnection.HTTP_OK) {
			    System.out.println("Server returned response code " + responseCode + ". Download failed.");
			}else {
				InputStream inputStream = httpCon != null ? httpCon.getInputStream() : httpsCon.getInputStream();
				result = convertInputStreamToString(inputStream);
			}
			if(httpCon != null) {
				httpCon.disconnect();
			}else {
				httpsCon.disconnect();
			}
			return result;
		} catch (MalformedURLException e) {
			System.out.println("not valid url: "+url);
		} catch (IOException e) {
			System.out.println("The connection goes wrong.");
		}
		return null;
	}
	
	private static String convertInputStreamToString(InputStream inputStream) {
		String text = new BufferedReader(
			      new InputStreamReader(inputStream, StandardCharsets.UTF_8))
			        .lines()
			        .collect(Collectors.joining("\r\n"));
		return text;
	}
	
	private String processURL(String url) {
		//check the url is stored or not
		HashMap<String, String> info = new HashMap<>();
		if(!sendHeadRequestAndGetResponse(url, info)) {
			return null;
		};
		type = info.get("type");
		userAgent = info.get("User-Agent");

		int size = info.get("size")!=null? Integer.parseInt(info.get("size")):0;
		modifiedTime = info.get("modifiedTime")!=null? Long.parseLong(info.get("modifiedTime")): 0;
		
		if(mapper.load(visitedURLItem.class, url)!=null) {
			System.out.println("The url is visited before: "+url);
			return null;
		}
		
		//if the path is refused by robots
		RuleItem theRule = RobotsHandler.getTheRule(url, userAgent, hostname);
		if(theRule!=null && !RobotsHandler.handleRobotFile(theRule)) {
			return null;
		}
		if(!checkDelayTime(url, userAgent, hostname)||!checkSize(url, size)||!checkType(url, type)) {
			return null;
		}; 
		 
		//get document
		String result = null;
//		if(!checkModifiedTime(url, modifiedTime)) {
//			result = Crawler.getItem().docTable.getDocument(url).getContent();
//		}
		result = getContent(url);
		return result;
	}
	
	public boolean sendHeadRequestAndGetResponse(String url, HashMap<String, String> info) {
		try {
			URL urlObj = new URL(url);
			HttpURLConnection httpCon = null;
			HttpsURLConnection httpsCon = null;
			if(url.startsWith("https")) {
				httpsCon = (HttpsURLConnection) urlObj.openConnection();
				httpsCon.setRequestMethod("HEAD");
				httpsCon.setRequestProperty("User-Agent", "cis455crawler");
				httpsCon.setInstanceFollowRedirects(false);
				httpsCon.connect();
				if(httpsCon.getResponseCode()!=HttpURLConnection.HTTP_OK) {
					System.out.println(url+"not 200 ok, the status code is "+httpsCon.getResponseCode());
					return false;
				}
				info.put("type", httpsCon.getContentType().split(";")[0]);
				info.put("size", String.valueOf(httpsCon.getContentLength()));
				info.put("modifiedTime", String.valueOf(httpsCon.getIfModifiedSince()));
				info.put("User-Agent", httpsCon.getRequestProperty("User-Agent"));
				httpsCon.disconnect();
			}else {
				httpCon = (HttpURLConnection) urlObj.openConnection();
				httpCon.setRequestMethod("HEAD");
				httpCon.setRequestProperty("User-Agent", "cis455crawler");
				httpCon.setInstanceFollowRedirects(false);
				httpCon.connect();
				if(httpCon.getResponseCode()!=HttpURLConnection.HTTP_OK) {
					System.out.println("not 200 ok, the status code is "+httpCon.getResponseCode());
					return false;
				}
				info.put("type", httpCon.getContentType().split(";")[0]);
				info.put("size", String.valueOf(httpCon.getContentLength()));
				info.put("modifiedTime", String.valueOf(httpCon.getIfModifiedSince()));
				info.put("User-Agent", httpCon.getRequestProperty("User-Agent"));
				httpCon.disconnect();
			}
			return true;
			
		} catch (MalformedURLException e) {
			System.out.println("invalid url: "+url);
			return false;
		} catch (IOException e) {
			return false;
		} catch(Exception e) {
			return false;
		}
	}
	
	private boolean checkDelayTime(String url, String userAgent, String hostname) {
		DomainItem item = mapper.load(DomainItem.class, hostname);
		if(item!=null && item.getDelay()>0) {
			int delay = item.getDelay();
			long diff = System.currentTimeMillis()-item.getLastAccessTime();
			if(diff<delay*1000) {
				SendMessageRequest send_msg_request = new SendMessageRequest()
		    	        .withQueueUrl(Crawler.getItem().fifoQueueUrl)
		    	        .withMessageBody(url)
		    	        .withMessageGroupId("baeldung-group-1");
				Crawler.getItem().sqs.sendMessage(send_msg_request);
				return false;
			}
		}
		return true;
	}
	
//	private boolean checkModifiedTime(String url, long modifiedTime) {
//		if(Crawler.getItem().urlToTimeTable.urlStored(url)) {
//			long accessedTime = Crawler.getItem().urlToTimeTable.getLastAccessTime(url);
//			if(modifiedTime != 0 && modifiedTime < accessedTime) {
//				logger.info(url+": not modified");
//				return false;
//			}
//		}
//		return true;
//	}
	
	private boolean checkType(String url, String type) {
		if(!type.equals("text/html")&&!type.equals("text/xml")&&!type.equals("application/xml")
				&&!type.equals("text/html")&&!type.endsWith("xml")) {
			logger.info(url+": type is not html or xml");
			return false;
		}
		return true;
	}
	
	private boolean checkSize(String url, int size) {
		if(size > Crawler.getItem().maxSize*Math.pow(2, 20)) {
			logger.info(url+": size of the file is too big");
			return false;
		}
		return true;
	}

}
