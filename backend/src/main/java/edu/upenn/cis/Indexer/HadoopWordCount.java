package edu.upenn.cis.Indexer;

import edu.upenn.cis.utils.CustomFileInputFormat;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class HadoopWordCount {
    private static final Log LOG = LogFactory.getLog(
            HadoopWordCount.class);

    public static class MyMapper extends Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String docId = key.toString();
            // remove tags and replace punctuation
            String htmlText = Jsoup.parse(value.toString()).text().replaceAll("[\\p{Punct}&&[^.]]+", " ");
            // lemmatize the text
            ArrayList<String> words = IndexerHelper.lemmatize(htmlText);

            int pos = 0;
            for (String word: words){
                if (word.matches("[a-zA-Z\\d]+")){
                    context.write(new Text(docId + " " + word), new Text(Integer.toString(pos)));
                }
                pos ++;
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text>{

        @Override
        public void reduce(Text keys, Iterable<Text> positions, Context context) throws IOException, InterruptedException {
            String docId = keys.toString().split(" ")[0];
            String word = keys.toString().split(" ")[1];
            List<String> poses = new ArrayList<>();
            for (Text tmp: positions){
                poses.add(tmp.toString());
            }
            context.write(new Text("(" + docId + "," + word + ")"), new Text(poses.toString()));
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
        Job job = Job.getInstance(config, "Word Count");

        job.setJarByClass(HadoopWordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setNumReduceTasks(1);
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

