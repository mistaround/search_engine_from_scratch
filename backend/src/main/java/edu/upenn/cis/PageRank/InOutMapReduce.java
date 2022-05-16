package edu.upenn.cis.PageRank;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import edu.upenn.cis.Indexer.IndexerHelper;
import edu.upenn.cis.utils.CustomFileInputFormat;
import edu.upenn.cis.utils.UrlToDocId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import edu.upenn.cis.utils.DocidToUrlItem;
import static edu.upenn.cis.Indexer.IndexerHelper.getDynamoDB;

public class InOutMapReduce {
    private static final Log LOG = LogFactory.getLog(
            edu.upenn.cis.Indexer.HadoopWordCount.class);

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            AmazonDynamoDB dynamoDB = getDynamoDB();
            DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);

            String docId = key.toString();
            String body = value.toString();

            // do not process the doc that is not in the forward index
            if (mapper.load(IndexerHelper.ForwardIndex.class, docId) == null){
                return;
            }

            DocidToUrlItem item = mapper.load(DocidToUrlItem.class, docId);
            if (item == null){
                return;
            }

            String url = item.getUrl();

            Document doc = Jsoup.parse(body, url);
            Elements links = doc.select("a");
            List<String> outLinks = new ArrayList<>();
            for (Element link: links){
                String outLink = link.attr("abs:href");
                if (!outLink.isEmpty() && !outLinks.contains(outLink) && outLink.getBytes(StandardCharsets.UTF_8).length < 2048){
//                    System.out.println(outLink);
                    outLinks.add(outLink);
                }
            }
            List<UrlToDocId> outLinkItems = new ArrayList<>();
            for (String tmp: outLinks){
                UrlToDocId tmpItem = new UrlToDocId();
                tmpItem.setUrl(tmp);
                outLinkItems.add(tmpItem);
            }
            Map<String, List<Object>> items = mapper.batchLoad(outLinkItems);
            if (items.get("url_to_docid") == null){
                // add self loop if no valid out edges
                context.write(new Text(docId), new Text(docId));
                return;
            }

            for (Object obj: items.get("url_to_docid")){
                UrlToDocId tmp = (UrlToDocId) obj;
                if (tmp.getDocid() != null){
                    context.write(new Text(docId), new Text(tmp.getDocid()));
                }
            }

        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text docId, Iterable<Text> outDocId, Context context) throws IOException, InterruptedException {
            System.out.println("Reducing: " + docId);
            ArrayList<String> outIdStrings = new ArrayList<>();
            for (Text tmp: outDocId){
                outIdStrings.add(tmp.toString());
            }

            String outString = String.join(",", outIdStrings);
            context.write(docId, new Text(outString));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.err.println("Start job...");
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: input_file output_file");
        }
        System.err.println("Set Job Instance");
        Job job = Job.getInstance(config, "Word Count");

        job.setJarByClass(InOutMapReduce.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(10);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CustomFileInputFormat.class);

        System.err.println(otherArgs[0]);
        System.err.println(otherArgs[1]);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean status = job.waitForCompletion(true);

        if (status){
            System.exit(0);
        }
        else {
            System.exit(1);
        }
    }
}
