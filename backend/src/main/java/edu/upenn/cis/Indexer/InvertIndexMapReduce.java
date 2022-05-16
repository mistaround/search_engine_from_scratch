package edu.upenn.cis.Indexer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
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

public class InvertIndexMapReduce {
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
                    if (word.length() < 30){
                        context.write(new Text(word), new Text("("+docId+"@"+occurs+")"));
                    }
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text word, Iterable<Text> occurs, Context context) throws IOException, InterruptedException {
            System.err.println(word);
            AmazonDynamoDB dynamoDB = IndexerHelper.getDynamoDB();
            DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);

            IndexerHelper.InvertIndex invertIndex = mapper.load(IndexerHelper.InvertIndex.class, word.toString());
            if (invertIndex == null){
                // if it's a new word, update num column
                IndexerHelper.RowCounts rowCounts = mapper.load(IndexerHelper.RowCounts.class, "InvertIndex");
                if (rowCounts == null){
                    rowCounts = new IndexerHelper.RowCounts();
                    rowCounts.setTableClassName("InvertIndex");
                }
                rowCounts.setRowCount(rowCounts.getRowCount() + 1);
                mapper.save(rowCounts);

                invertIndex = new IndexerHelper.InvertIndex();
                invertIndex.setWord(word.toString());
                invertIndex.setnDoc(0);
            }
            // add the docId to the corresponding invert index.
            for (Text occur: occurs){
                String[] tmp = occur.toString().split("@");
                String docId = tmp[0].substring(1);
                int count = tmp[1].trim().substring(1, tmp[1].length() - 2).split(",").length;
                invertIndex.addInvertIndex(docId, count);
            }

            int minCount = 2;

            // making sure that the invert index item does not exceed limit.
            while (invertIndex.getInvertIndex().size() > 0){
                try{
                    mapper.save(invertIndex);
                    break;
                } catch (Exception e){
                    invertIndex.shrink(minCount);
                    minCount ++;
                }
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
        Job job = Job.getInstance(config, "Invert Index");

        job.setJarByClass(InvertIndexMapReduce.class);
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