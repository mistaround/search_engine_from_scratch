package edu.upenn.cis.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class CustomLineRecordReader extends RecordReader {
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private Text key = new Text();
    private Text nextKey = new Text();
    private Text value = new Text();
    private String lineSeparator = "\n";



    /**
     * From Design Pattern, O'Reilly...
     * This method takes as arguments the map task’s assigned InputSplit and
     * TaskAttemptContext, and prepares the record reader. For file-based input
     * formats, this is a good place to seek to the byte position in the file to
     * begin reading.
     */
    @Override
    public void initialize(
            InputSplit genericSplit,
            TaskAttemptContext context)
            throws IOException {

        // This InputSplit is a FileInputSplit
        FileSplit split = (FileSplit) genericSplit;

        // Retrieve configuration, and Max allowed
        // bytes for a single record
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(
                "mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);

        // Split "S" is responsible for all records
        // starting from "start" and "end" positions
        start = split.getStart();
        end = start + split.getLength();

        // Retrieve file containing Split "S"
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        // If Split "S" starts at byte 0, first line will be processed
        // If Split "S" does not start at byte 0, first line has been already
        // processed by "S-1" and therefore needs to be silently ignored
        boolean skipFirstLine = false;

        if (start != 0) {
            skipFirstLine = true;
            // Set the file pointer at "start - 1" position.
            // This is to make sure we won't miss any line
            // It could happen if "start" is located on a EOL
            --start;
            fileIn.seek(start);
        }

        in = new LineReader(fileIn, job);

        // If first line needs to be skipped, read first line
        // and stores its content to a dummy Text
        if (skipFirstLine) {
            Text dummy = new Text();
            // Reset "start" to "start + line offset"
            start += in.readLine(dummy, 0,
                    (int) Math.min(
                            (long) Integer.MAX_VALUE,
                            end - start));
        }
        this.pos = start;

        Text tmpBuffer = new Text();
        while (true){
            int newSize = in.readLine(tmpBuffer, maxLineLength,
                    Math.max((int) Math.min(
                                    Integer.MAX_VALUE, end - pos),
                            maxLineLength));
            this.pos += newSize;
            if (tmpBuffer.toString().trim().startsWith("***DOCID")){
                break;
            }
        }

        nextKey.set(tmpBuffer.toString().trim().substring(9).trim());
    }

    /**
     * From Design Pattern, O'Reilly...
     * Like the corresponding method of the InputFormat class, this reads a
     * single key/ value pair and returns true until the data is consumed.
     */
    @Override
    public boolean nextKeyValue() throws IOException {

        key.set(nextKey.toString());
        int newSize = 0;
        int totalSizeRead = 0;
        Text tmpBuffer = new Text();
        value.clear();
        while (pos < end){

            newSize = in.readLine(tmpBuffer, maxLineLength,
                    Math.max((int) Math.min(
                                    Integer.MAX_VALUE, end - pos),
                            maxLineLength));

            pos += newSize;
            totalSizeRead += newSize;
            if (tmpBuffer.toString().trim().startsWith("***DOCID:")){
                nextKey.set(tmpBuffer.toString().trim().substring(9).trim());
//                value.set(value.toString().trim());
                break;
            }
            tmpBuffer.append(lineSeparator.getBytes(StandardCharsets.UTF_8), 0, lineSeparator.getBytes(StandardCharsets.UTF_8).length);
            value.append(tmpBuffer.copyBytes(), 0, tmpBuffer.getLength());

        }
        if (totalSizeRead == 0){
            key = null;
            value = null;
            return false;
        } else {
            if (pos >= end){
                key.set(nextKey.toString());
            }
            return true;
        }
    }
//    @Override
//    public boolean nextKeyValue() throws IOException {
//        // Current offset is the key
//        key.set(nextKey);
//
//        int newSize = 0;
//
//        // Make sure we get at least one record that starts in this Split
//        while (pos < end) {
//            Text tmpBuffer = new Text();
//            // Read first line and store its content to "value"
//            newSize = in.readLine(tmpBuffer, maxLineLength,
//                    Math.max((int) Math.min(
//                                    Integer.MAX_VALUE, end - pos),
//                            maxLineLength));
//
//            if (tmpBuffer.toString().startsWith("***DOCID:")){
//                nextKey.set(tmpBuffer.toString().substring(9).trim());
////                value.set(value.toString().trim());
//            }
//            else {
//                value.set(tmpBuffer.toString());
//                if (newSize == 0) {
//                    break;
//                }
//
//                if (newSize < maxLineLength) {
//                    break;
//                }
//            }
//            pos += newSize;
//        }
//
//        if (newSize == 0) {
//            // We've reached end of Split
//            key = null;
//            value = null;
//            return false;
//        } else {
//            // Tell Hadoop a new line has been found
//            // key / value will be retrieved by
//            // getCurrentKey getCurrentValue methods
//            return true;
//        }
//    }

    /**
     * From Design Pattern, O'Reilly...
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public Text getCurrentKey() throws IOException,
            InterruptedException {

        return key;
    }

    /**
     * From Design Pattern, O'Reilly...
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * From Design Pattern, O'Reilly...
     * Like the corresponding method of the InputFormat class, this is an
     * optional method used by the framework for metrics gathering.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    /**
     * From Design Pattern, O'Reilly...
     * This method is used by the framework for cleanup after there are no more
     * key/value pairs to process.
     */
    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}
