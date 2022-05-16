package edu.upenn.cis.Indexer;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class ForwardIndexMapReduce {
    private static final Log LOG = LogFactory.getLog(
            HadoopWordCount.class);

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] curLine = value.toString().split("\\)");
            String docId;
            String word;
            if (curLine.length == 2){
                String tmpKey = curLine[0].substring(1);
                String occurs = curLine[1].trim();
                String[] keys = tmpKey.split(",");
                if (keys.length == 2){
                    docId = keys[0];
                    word = keys[1];
                    context.write(new Text(docId), new Text("("+word+"@"+occurs+")"));
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text docId, Iterable<Text> wordOccurs, Context context) throws IOException, InterruptedException {
            System.err.println(docId);
            AWSCredentials credentials = new BasicAWSCredentials(
                    IndexerHelper.accessKey,
                    IndexerHelper.secretKey
            );
            AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .build();
            DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);

            IndexerHelper.ForwardIndex forwardIndex = mapper.load(IndexerHelper.ForwardIndex.class, docId.toString());
            if (forwardIndex == null){
                // if it's a new doc, update num column
                IndexerHelper.RowCounts rowCounts = mapper.load(IndexerHelper.RowCounts.class, "ForwardIndex");
                if (rowCounts == null){
                    rowCounts = new IndexerHelper.RowCounts();
                    rowCounts.setTableClassName("ForwardIndex");
                }
                rowCounts.setRowCount(rowCounts.getRowCount() + 1);
                mapper.save(rowCounts);

                forwardIndex = new IndexerHelper.ForwardIndex();
                forwardIndex.setDocId(docId.toString());
            }
            // create the forward index item
            for (Text occur: wordOccurs){
                String[] tmp = occur.toString().split("@");
                String word = tmp[0].substring(1);
                String[] occurs = tmp[1].trim().substring(1, tmp[1].length() - 2).split(",");

                List<Integer> positions = new ArrayList<>();
                for (String pos: occurs){
                    positions.add(Integer.parseInt(pos.trim()));
                }
                Collections.sort(positions);
                forwardIndex.addForwardIndex(word, positions.size());
                forwardIndex.addHitLists(word, positions);
            }

            // making sure the item does not exceed size.
            int maxL = 4;
            boolean saved = false;
            while (maxL >= 0){
                try {
                    mapper.save(forwardIndex);
                    saved = true;
                    break;
                } catch (Exception e){
                    forwardIndex.shrinkHitLists(maxL);
                    maxL --;
                    e.printStackTrace();
                }
            }
            if (!saved){
                IndexerHelper.RowCounts rowCounts = mapper.load(IndexerHelper.RowCounts.class, "ForwardIndex");
                rowCounts.setRowCount(rowCounts.getRowCount() - 1);
                mapper.save(rowCounts);
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.err.println("Start job...");
        System.err.println(HadoopWordCount.class.getName());
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: input_file output_file");
        }
        System.err.println("Set Job Instance");
        Job job = Job.getInstance(config, "Forward Index");

        job.setJarByClass(ForwardIndexMapReduce.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(10);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

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
