package edu.upenn.cis.utils;

import edu.upenn.cis.Indexer.IndexerHelper;

import java.io.IOException;
import java.util.*;

public class MyPlayGround {

    public static void main(String[] args) throws IOException {
        long start = new Date().getTime();
        String query = "Ukraine";
        List<String> urls = IndexerHelper.processQuery(query, 0, 10);
        System.out.println("Search results for: " + query);
        for (String url: urls){
            System.out.println(url);
        }
        System.out.println(new Date().getTime() - start);
//        IndexerHelper.createTable(IndexerHelper.InvertIndex.class);
    }
}