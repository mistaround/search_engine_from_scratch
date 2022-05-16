package edu.upenn.cis.crawler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;

import edu.upenn.cis.crawler.utils.URLInfo;
import edu.upenn.cis.crawler.storage.ChecksumItem;
import edu.upenn.cis.crawler.storage.DocidToUrlItem;
import edu.upenn.cis.crawler.storage.DomainItem;
import edu.upenn.cis.crawler.storage.Storage;
import edu.upenn.cis.crawler.storage.UrlToDocIdItem;
import edu.upenn.cis.crawler.storage.fileIdToDocIdItem;
import edu.upenn.cis.crawler.storage.visitedURLItem;
import edu.upenn.cis.crawler.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.crawler.stormlite.TopologyContext;
import edu.upenn.cis.crawler.stormlite.bolt.IRichBolt;
import edu.upenn.cis.crawler.stormlite.bolt.OutputCollector;
import edu.upenn.cis.crawler.stormlite.routers.IStreamRouter;
import edu.upenn.cis.crawler.stormlite.tuple.Fields;
import edu.upenn.cis.crawler.stormlite.tuple.Tuple;
import edu.upenn.cis.crawler.stormlite.tuple.Values;

public class LinkExtractorBolt implements IRichBolt{
	static Logger logger = LogManager.getLogger(LinkExtractorBolt.class);

	Fields schema = new Fields("extractedUrl"); 
	
    String executorId = UUID.randomUUID().toString();
    
    DynamoDBMapper mapper;
    
    String fileId = generateFileId();

    private OutputCollector collector;

    public LinkExtractorBolt() {}
    
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
		String result = input.getStringByField("document");
		String url = input.getStringByField("url");
		String type = input.getStringByField("type");
		
		if(result==null) { return; }
		String hostname = new URLInfo(url).getHostName();
		//get the hash
		byte[] digest; 
		try {
			digest = getContentHash(result);
			ChecksumItem item = mapper.load(ChecksumItem.class, new String(digest));
			if(item==null) {
	        	checkDocLimits();
	        	updateDatabase(url, result, digest, hostname, type);
	        	if(type.equals("text/html")) { 
	        		addExtractUrl(result, url);
	        	}
	        	checkDocLimits();
	        }
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch(Exception e) {
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
	
	public String generateFileId() {
		return UUID.randomUUID().toString();
	}
	
	public ArrayList<String> parseUrl(String content, String url) {
		Document doc = Jsoup.parse(content, url);
		Elements links = doc.select("a");
		ArrayList<String> urls = new ArrayList<>();
		for (Element link: links){
            String nextLink = link.attr("abs:href");
            urls.add(nextLink); 
        }

        return urls;
	}
	private byte[] getContentHash(String result) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("MD5");
	    md.update(result.getBytes());
	    byte[] digest = md.digest();
	    return digest;
	}
	
	private void checkDocLimits() {
		if(Crawler.getItem().getNumOfFile()>=Crawler.getItem().count) {
			Crawler.getItem().notifyThreadExited();
    	}
	}
	
	private void updateDatabase(String url, String result, byte[] digest, String domain, String type) throws IOException {
		
    	//if it not contains the hash,  add it into contentSeen table
		ChecksumItem checksumItem = new ChecksumItem();
		checksumItem.setHash(new String(digest));

		UrlToDocIdItem urlToDocidItem = new UrlToDocIdItem();
		urlToDocidItem.setUrl(url);
		urlToDocidItem.setDocid();
		
		String docid =  urlToDocidItem.getDocid();
		
		
		DocidToUrlItem docidToUrlItem = new DocidToUrlItem();
		docidToUrlItem.setUrl(url);
		docidToUrlItem.setDocid(docid);
		
		fileIdToDocIdItem file_doc = new fileIdToDocIdItem();
		file_doc.setDocId(docid);
		
		String filename = "temp"+this.fileId+".txt";
		File myObj = new File(filename);
	    if (myObj.createNewFile()) {
	      System.out.println("File created: " + myObj.getName());
//	      Crawler.getItem().fileId=UUID.randomUUID().toString();
	    } else {
	      System.out.println("File already exists."+url);
	    }
	    file_doc.setFileId(this.fileId);
	    
   	 	if(myObj.length()>50000000) {
	    	
			((Storage)Crawler.getItem().db).s3client.putObject(
			credentialSet.documentBucket,
			file_doc.getFileId(),
			new File("temp"+file_doc.getFileId()+".txt"));
			
			System.out.println("File is full");
			myObj.delete();	
			this.fileId = generateFileId();
	    }
   	 	filename = "temp"+this.fileId+".txt";
   	 	FileWriter myWriter;
	    myWriter = new FileWriter(filename, true);
	 	myWriter.write("\r\n ***DOCID: "+docid+"\r\n"+result+"\r\n");
	 	myWriter.close();
	 	
		visitedURLItem urlItem = new visitedURLItem();
		urlItem.setUrl(url);
		
		DomainItem item = mapper.load(DomainItem.class, domain);
    	if(item!=null) {
    		item.setLastAccessTime(System.currentTimeMillis());
    	}else {
    		item = new DomainItem();
    		item.setDelay(RobotsHandler.getDelay());
    		item.setDomainName(domain);
    		item.setLastAccessTime(System.currentTimeMillis());
    	}
    	mapper.batchSave(checksumItem, urlToDocidItem, docidToUrlItem, urlItem, item, file_doc);
    	logger.info(url+": downloading");
		Crawler.getItem().incCount();
		
		System.out.println("num of file: "+Crawler.getItem().getNumOfFile());
	}
	private void addExtractUrl(String result, String url) {
		ArrayList<String> urls = parseUrl(result, url);
    	//store urls to queue
    	for(String one: urls) {
//    		String processedUrl = processURL(one, url);
//    		if(processedUrl.length()==0) { continue; }
//    		if(!one.contains(".")&&!processedUrl.endsWith("/")) { processedUrl+="/"; }
    		collector.emit(new Values<Object>(one));
    	}
	}
	private String processURL(String one, String url) {
		String processedUrl = "";
		if(one.startsWith("http")) {
			processedUrl = one;
		}else if(one.startsWith("//")) {
			if(url.startsWith("https")) {
				processedUrl = "https:"+one;
			}else {
				processedUrl = "http:"+one;
			}
		}else if(one.startsWith("/")){
			processedUrl = url.endsWith("/")?url.substring(0, url.length()-1)+one:url+one;
		}else {
			if(url.endsWith("/")) {
				processedUrl=url+one;
			}else {
				String[] parts = url.split("/");
				parts[parts.length-1]=one;
				processedUrl = String.join("/", parts);
			}
		}
		return processedUrl;
	}

}
