package edu.upenn.cis.PageRank;

import org.apache.hadoop.util.ToolRunner;

public class PageRankMain {

    public static void main(String[] args) throws Exception {

        int iterations = Integer.parseInt(args[2]);
        System.err.println("Calculate pagerank for " + iterations + " iterations.");
        for (int i = 0; i < iterations; i++) {
            String[] dirs = new String[] {args[0], args[1] + "/" + i};
            int status = ToolRunner.run(new PageRankMapReduce(), dirs);
            if (status == 1) {
                System.err.println("PageRank failed at Iteration: " + i);
                System.exit(status);
            }
        }
        System.exit(0);
    }
}