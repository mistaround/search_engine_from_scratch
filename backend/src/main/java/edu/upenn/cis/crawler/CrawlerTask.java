package edu.upenn.cis.crawler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.upenn.cis.crawler.stormlite.Config;
import edu.upenn.cis.crawler.stormlite.LocalCluster;
import edu.upenn.cis.crawler.stormlite.Topology;
import edu.upenn.cis.crawler.stormlite.TopologyBuilder;
import edu.upenn.cis.crawler.stormlite.tuple.Fields;

public class CrawlerTask {
	static Logger log = LogManager.getLogger(CrawlerTask.class);

	private static final String QUEUE_SPOUT = "QUEUE_SPOUT";
    private static final String DOC_FETCHER_BOLT = "DOC_FETCHER_BOLT";
    private static final String LINK_EXTRACTOR_BOLT = "LINK_EXTRACTOR_BOLT";
    private static final String FILTER_URL_BOLT = "FILTER_URL_BOLT";
    
    public static void build() throws Exception{
    	Config config = new Config();

        CrawlerQueueSpout spout = new CrawlerQueueSpout();
        DocumentFetcherBolt docFetcherBolt = new DocumentFetcherBolt(); 
        LinkExtractorBolt linkExtractorBolt = new LinkExtractorBolt();
        FilterUrlUpdateQueueBolt filterUrlUpdateQueueBolt = new FilterUrlUpdateQueueBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(QUEUE_SPOUT, spout, 1);
        builder.setBolt(DOC_FETCHER_BOLT, docFetcherBolt, 1).fieldsGrouping(QUEUE_SPOUT, new Fields("domain"));
        builder.setBolt(LINK_EXTRACTOR_BOLT, linkExtractorBolt, 1).shuffleGrouping(DOC_FETCHER_BOLT);
        builder.setBolt(FILTER_URL_BOLT, filterUrlUpdateQueueBolt, 1).shuffleGrouping(LINK_EXTRACTOR_BOLT);

        
        LocalCluster cluster = new LocalCluster();
        Topology topo = builder.createTopology();

        ObjectMapper mapper = new ObjectMapper();
        
        cluster.submitTopology("test", config, 
        		builder.createTopology());
        
    }

}
