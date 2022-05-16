package edu.upenn.cis.PageRank;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

import static edu.upenn.cis.Indexer.IndexerHelper.getDynamoDB;

public class PageRankMapReduce extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(
            edu.upenn.cis.Indexer.HadoopWordCount.class);

    private static double dumpingFactor = 0.7;

    public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\s+");
            String docId = line[0];
            String[] outDocIds;
            outDocIds = line[1].split(",");

            DynamoDBMapper mapper = new DynamoDBMapper(getDynamoDB());

            PageRankHelper.PageRankItem pagerank = mapper.load(PageRankHelper.PageRankItem.class, docId);
            if (pagerank == null){
                pagerank = new PageRankHelper.PageRankItem();
                pagerank.setDocId(docId);
                pagerank.setPagerank(1);
            }

            for (String outDocId: outDocIds){
                context.write(new Text(outDocId.strip()),
                        new DoubleWritable(pagerank.getPagerank() / outDocIds.length));
            }

        }
    }

    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, Text> {

        @Override
        public void reduce(Text docId, Iterable<DoubleWritable> scores, Context context) throws IOException, InterruptedException {
            double finalScore = 0;
            for (DoubleWritable tmp: scores){
                finalScore += tmp.get();
            }
            DynamoDBMapper mapper = new DynamoDBMapper(getDynamoDB());
            PageRankHelper.PageRankItem item = mapper.load(PageRankHelper.PageRankItem.class, docId.toString());
            if (item == null){
                item = new PageRankHelper.PageRankItem();
                item.setDocId(docId.toString());
            }
            item.setPagerank(dumpingFactor * finalScore + (1 - dumpingFactor));

            mapper.save(item);
        }

    }

    @Override
    public int run(String[] args) throws Exception {
        System.err.println("Start job...");
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.out.println("Usage: input_file output_file");
        }
        System.err.println("Set Job Instance");
        Job job = Job.getInstance(config, "Word Count");

        job.setJarByClass(PageRankMapReduce.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(10);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.err.println(otherArgs[0]);
        System.err.println(otherArgs[1]);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        boolean status = job.waitForCompletion(true);

        if (status){
            return 0;
        }
        else {
            return 1;
        }
    }
}
