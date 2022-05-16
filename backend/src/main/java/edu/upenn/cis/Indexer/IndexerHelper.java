package edu.upenn.cis.Indexer;


import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static java.util.stream.Collectors.toMap;

import static edu.upenn.cis.PageRank.PageRankHelper.PageRankItem;
import edu.upenn.cis.utils.DocidToUrlItem;
import edu.upenn.cis.utils.FileToDoc;
import edu.upenn.cis.utils.UrlToDocId;
import org.apache.commons.math3.linear.ArrayRealVector;

import edu.stanford.nlp.ling.*;
import edu.stanford.nlp.pipeline.*;

public class IndexerHelper {
    public static String accessKey = "AKIAX5K2P2746CPKER5X";
    public static String secretKey = "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";
    public static String crawlBucketName = "crawleddocuments";

    @DynamoDBTable(tableName = "RowCounts")
    public static class RowCounts{

        private String tableClassName;
        private int rowCount;

        @DynamoDBHashKey(attributeName = "tableClassName")
        public String getTableClassName() {return tableClassName;}

        public void setTableClassName(String tableClassName) {this.tableClassName = tableClassName;}

        @DynamoDBAttribute(attributeName = "rowCount")
        public int getRowCount() {
            return rowCount;
        }

        public void setRowCount(int rowCount) {this.rowCount = rowCount;}

        public RowCounts(){}

    }
    @DynamoDBTable(tableName = "ForwardIndex")
    public static class ForwardIndex{

        private String docId;
        // word to numOccur
        private HashMap<String, Integer> forwardIndex = new HashMap<>();
        // word hit lists
        private HashMap<String, List<Integer>> hitLists = new HashMap<>();
        // square norm of the words occurrences, used for calculating term frequency.
        private int squareNorm;

        @DynamoDBAttribute(attributeName = "hitLists")
        public HashMap<String, List<Integer>> getHitLists() {return hitLists;}

        public void setHitLists(HashMap<String, List<Integer>> hitLists) {this.hitLists = hitLists;}

        @DynamoDBAttribute(attributeName = "forwardIndex")
        public HashMap<String, Integer> getForwardIndex() { return forwardIndex;}

        public void setForwardIndex(HashMap<String, Integer> forwardIndex) {this.forwardIndex = forwardIndex;}

        @DynamoDBAttribute(attributeName = "squareNorm")
        public int getSquareNorm() {return squareNorm;}

        public void setSquareNorm(int squareNorm) {this.squareNorm = squareNorm;}

        @DynamoDBHashKey(attributeName = "docId")
        public String getDocId() {return docId;}

        public void setDocId(String docId) {this.docId = docId;}

        public ForwardIndex(){}

        public void addForwardIndex(String word, Integer numOccurs){
            if (this.forwardIndex.get(word) != null) {
                int orig = this.forwardIndex.get(word);
                this.squareNorm -= Math.pow(orig, 2);
            }
            this.squareNorm += Math.pow(numOccurs, 2);
            this.forwardIndex.put(word, numOccurs);
        }

        public void addHitLists(String word, List<Integer> occurs){
            if (occurs.size() > 5){
                occurs = occurs.subList(0, 5);
            }
            this.hitLists.put(word, occurs);
        }

        public void shrinkHitLists(int maxL){
            if (maxL == 0){
                this.hitLists.clear();
            }
            else {
                for (String word: this.forwardIndex.keySet()){
                    List<Integer> curPos = this.hitLists.get(word);
                    Collections.sort(curPos);

                    if (curPos.size() > maxL){
                        this.hitLists.put(word, curPos.subList(0, maxL));
                    }
                }
            }
        }

    }

    @DynamoDBTable(tableName = "InvertIndex")
    public static class InvertIndex{
        private String word;
        private HashMap<String, Integer> invertIndex = new HashMap<>();
        private Integer nDoc;

        @DynamoDBAttribute(attributeName = "invertIndex")
        public HashMap<String, Integer> getInvertIndex() {return invertIndex;}

        public void setInvertIndex(HashMap<String, Integer> invertIndex) {this.invertIndex = invertIndex;}

        @DynamoDBHashKey(attributeName = "word")
        public String getWord() {return word;}

        public void setWord(String word) {this.word = word;}

        @DynamoDBAttribute(attributeName = "nDoc")
        public Integer getnDoc() {return nDoc;}

        public void setnDoc(Integer nDoc) {this.nDoc = nDoc;}

        public InvertIndex(){}

        public void addInvertIndex(String docId, int numOccur){
            if (!this.invertIndex.containsKey(docId)){
                this.nDoc += 1;
            }
            this.invertIndex.put(docId, numOccur);
        }

        public void shrink(int minCount){
            HashMap<String, Integer> copy = new HashMap<>(this.invertIndex);
            for (String docId: copy.keySet()){
                if (this.invertIndex.get(docId) < minCount){
                    this.invertIndex.remove(docId);
                }
            }
        }

    }

    public static AmazonDynamoDB getDynamoDB(){
        AWSCredentials credentials = new BasicAWSCredentials(
                accessKey,
                secretKey
        );

        return AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
    }

    public static AmazonS3 getAmazonS3Client(){
        AWSCredentials credentials = new BasicAWSCredentials(
                accessKey,
                secretKey
        );

        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withRegion(Regions.US_EAST_1)
                .build();
    }

    // given a string, we return a list of lemmatized words.
    public static ArrayList<String> lemmatize(String content){

        ArrayList<String> words = new ArrayList<>();
        Properties props = new Properties();
        // set the list of annotators to run
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma");
        // build pipeline
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        // create a document object
        CoreDocument document = pipeline.processToCoreDocument(content);

        // display tokens
        for (CoreLabel tok : document.tokens()) {
            words.add(tok.lemma().toLowerCase(Locale.ROOT));
        }
        return words;
    }

    // given a search query, we return the documents in decreasing order of the score.
    public static List<String> processQuery(String query, int left, int right){
        System.out.println("Query For: " + query);
        long start = new Date().getTime();

        AmazonDynamoDB dynamoDB = getDynamoDB();

        ArrayList<String> words = lemmatize(query);

        Set<String> allDocIds = getAllDocId(words, dynamoDB);
        if (allDocIds == null || allDocIds.isEmpty()) {
            List<String> urls = new ArrayList<>();
            urls.add(String.valueOf(0));
            return urls;
        }
        // System.out.println(allDocIds.size());
        HashMap<String, Double> docScore = new HashMap<>();
        ArrayRealVector queryWeights = getQueryWeights(words, dynamoDB);

        HashMap<String, ForwardIndex> forwardIndices = getBatchForwardIndices(allDocIds, dynamoDB);
        HashMap<String, PageRankItem> pageRankItems = getBatchPageRank(allDocIds, dynamoDB);
        // System.out.println(queryWeights);
        for (String docId: forwardIndices.keySet()){
            ForwardIndex forwardIndex = forwardIndices.get(docId);
            PageRankItem pageRankItem = pageRankItems.get(docId);
            ArrayRealVector docWeights = getDocWeights(forwardIndex, words);
            double score = docWeights.dotProduct(queryWeights);
            List<Double> avgDist = getWordsDistance(forwardIndex, words);

            if (avgDist != null){
                for (Double dist: avgDist){
                    score += 5 / dist;
                }
            }

            double pagerank = pageRankItem == null ? 0 : pageRankItem.getPagerank();

            docScore.put(docId, pagerank * score);

        }
        docScore = docScore.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                        LinkedHashMap::new));

        int total = docScore.keySet().size();
        if (total == 0) {
            List<String> urls = new ArrayList<>();
            urls.add(String.valueOf(total));
            return urls;
        }
        if (left >= total) {
            List<String> urls = new ArrayList<>();
            urls.add(String.valueOf(total));
            return urls;
        }
        if (right >= total) {
            right = total - 1;
        }
        List<String> docIds = new ArrayList<>(docScore.keySet()).subList(left, right);
        List<String> urls = getUrls(docIds, dynamoDB);
        urls.add(String.valueOf(total));
        System.out.println("Query Time: " + (new Date().getTime() - start));
        return urls;
    }

    // batch load the pagerank item give docIds.
    public static HashMap<String, PageRankItem> getBatchPageRank(Set<String> docIds, AmazonDynamoDB dynamoDB){
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
        ArrayList<PageRankItem> itemsToGet = new ArrayList<>();
        for (String docId: docIds){
            PageRankItem cur = new PageRankItem();
            cur.setDocId(docId);
            itemsToGet.add(cur);
        }
        Map<String, List<Object>> items = mapper.batchLoad(itemsToGet);
        HashMap<String, PageRankItem> map = new HashMap<>();
        for (Object obj: items.get("PageRank")){
            PageRankItem item = (PageRankItem) obj;
            map.put(item.getDocId(), item);
        }
        return map;
    }

    // given a list of docIds, we do batch load to get the forward indices for the documents.
    public static HashMap<String, ForwardIndex> getBatchForwardIndices(Set<String> docIds, AmazonDynamoDB dynamoDB){
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
        ArrayList<ForwardIndex> itemsToGet = new ArrayList<>();
        for (String docId: docIds){
            ForwardIndex cur = new ForwardIndex();
            cur.setDocId(docId);
            itemsToGet.add(cur);
        }
        Map<String, List<Object>> items = mapper.batchLoad(itemsToGet);
        HashMap<String, ForwardIndex> map = new HashMap<>();
        for (Object obj: items.get("ForwardIndex")){
            ForwardIndex forwardIndex = (ForwardIndex) obj;
            map.put(forwardIndex.getDocId(), forwardIndex);
        }
        return map;
    }

    // calculate average word distance for a document given a query
    public static List<Double> getWordsDistance(ForwardIndex forwardIndex, List<String> words){

        if (forwardIndex == null || forwardIndex.getHitLists() == null){
            return null;
        }
        HashMap<String, List<Integer>> hitLists = forwardIndex.getHitLists();
        List<Double> avgDist = new ArrayList<>();

        for (int i=0;i<words.size()-1;i++){
            List<Integer> curWordHitList = hitLists.getOrDefault(words.get(i), new ArrayList<>());
            List<Integer> nextWordHitList = hitLists.getOrDefault(words.get(i+1), new ArrayList<>());
            double distances = 0;

            for (int pos1: curWordHitList){
                int minLength = Integer.MAX_VALUE;
                for (int pos2: nextWordHitList){
                    int dist = Math.abs(pos2 - pos1);
                    if (dist < minLength){
                        minLength = dist;
                    }
                }
                distances += minLength;
            }
            if (curWordHitList.size() > 0){
                avgDist.add(distances / curWordHitList.size());
            } else {
                avgDist.add((double) Integer.MAX_VALUE);
            }
        }
        return avgDist;
    }

    // get the document vector
    public static ArrayRealVector getDocWeights(ForwardIndex forwardIndex, List<String> words){
        ArrayRealVector docWeights = new ArrayRealVector();
        for (String word: words) {
            docWeights = (ArrayRealVector) docWeights.append(getTermFreq(forwardIndex, word));
        }
        return docWeights;
    }

    // get the query vector
    public static ArrayRealVector getQueryWeights(List<String> words, AmazonDynamoDB dynamoDB){
        ArrayRealVector queryWeights = new ArrayRealVector();
        for (String word: words){
            if (word.equals("of") || word.equals("the")){
                queryWeights = (ArrayRealVector) queryWeights.append(0);
            } else {
                queryWeights = (ArrayRealVector) queryWeights.append(getInverseDocFreq(word, dynamoDB));
            }

        }
        return queryWeights;
    }

    // give a word, we return the idf for that word
    public static Double getInverseDocFreq(String word, AmazonDynamoDB dynamoDB){
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);

        InvertIndex invertIndex = mapper.load(InvertIndex.class, word);
        RowCounts rowCounts = mapper.load(RowCounts.class, "ForwardIndex");
        int totalDoc = rowCounts.getRowCount();
        if (invertIndex == null) {
            System.out.println("word: " + word + " is not indexed.");
            return 0.0;
        }
        return  Math.log10((double) totalDoc / invertIndex.getnDoc());
    }

    // given a forward index of a doc and a word, we return the term frequency
    public static double getTermFreq(ForwardIndex forwardIndex, String word){
        if (forwardIndex == null || forwardIndex.getForwardIndex() == null) {
            return 0.0;
        }

        double norm = Math.pow(forwardIndex.getSquareNorm(), 0.5);
        if (forwardIndex.getForwardIndex().get(word) == null){
            return 0.0;
        }
        return forwardIndex.getForwardIndex().get(word) / norm;
    }

    // given a word list, we find all the docIds where the words occur top 30 most frequent;y for each word.
    public static Set<String> getAllDocId(List<String> words, AmazonDynamoDB dynamoDB){
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);

        Set<String> docIds = new HashSet<>();
        for (String word: words){
            InvertIndex invertIndex = mapper.load(InvertIndex.class, word);
            if (invertIndex != null){
                HashMap<String, Integer> sortedInvertIndex = invertIndex.getInvertIndex()
                        .entrySet()
                        .stream()
                        .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2,
                                LinkedHashMap::new));
                if (sortedInvertIndex.size() > 30){
                    docIds.addAll(new ArrayList<>(sortedInvertIndex.keySet()).subList(0, 30));
                } else {
                    docIds.addAll(sortedInvertIndex.keySet());
                }
            }
        }
        return docIds;
    }

    // given a list of docIds, we return the list of corresponding urls.
    public static List<String> getUrls(List<String> docIds, AmazonDynamoDB dynamoDB){
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
        List<String> urls = new ArrayList<>();
        for (String docId: docIds){
            urls.add(mapper.load(DocidToUrlItem.class, docId).getUrl());
        }
        return urls;
    }

    // given an url, we either download it directly from the internet or get it from S3.
    // downloading directly from the internet is much faster than getting from S3.
    public static String getUrlContent(String url, boolean download) throws IOException {
        if (download){
            URL newUrl = new URL(url);
            HttpURLConnection connection;
            if (url.startsWith("https://")){
                connection = (HttpsURLConnection) newUrl.openConnection();
            }
            else {
                connection = (HttpURLConnection) newUrl.openConnection();
            }
            return new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } else {
            AmazonDynamoDB dynamoDB = getDynamoDB();
            AmazonS3 s3client = getAmazonS3Client();

            DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
            String docId = mapper.load(UrlToDocId.class, url).getDocid();
            String fileId = mapper.load(FileToDoc.class, docId).getFileId();

            S3Object s3object = s3client.getObject(crawlBucketName, fileId);
            S3ObjectInputStream inputStream = s3object.getObjectContent();
            String fileContent = new String(inputStream.readAllBytes());

            int startIndex = fileContent.indexOf(docId) + docId.length();
            int endIndex = fileContent.indexOf("***DOCID", startIndex);
            if (endIndex != -1)
                return fileContent.substring(startIndex, endIndex);
            else
                return fileContent.substring(startIndex);
        }
    }

    // create a dynamoDB table
    public static void createTable(Class c){
        AmazonDynamoDB dynamoDB = getDynamoDB();
        DynamoDBMapper mapper = new DynamoDBMapper(dynamoDB);
        CreateTableRequest req = mapper.generateCreateTableRequest(c);
        req.setProvisionedThroughput(new ProvisionedThroughput(5L, 5L));
        dynamoDB.createTable(req);
    }
}
