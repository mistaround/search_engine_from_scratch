package edu.upenn.cis.Server;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.upenn.cis.Indexer.IndexerHelper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Date;

import static spark.Spark.*;

public class Server {
    private static List<Result> makeResultFromUrl(List<String> urls, String query) {
        long start = new Date().getTime();
        List<Result> results = new ArrayList<>();
        for (String url: urls) {
            try {
                Document document = Jsoup.parse(IndexerHelper.getUrlContent(url, true));
                System.out.println(url);
                // Document document = Jsoup.connect(url).maxBodySize(0).timeout(1000).get();
                String title = document.title();
                // String title = "";
                // Description v2
                String description = makeDescription(document, query);
                // String description = "";
                results.add(new Result(title, url, description));
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Exception in makeResultFromUrl");
                continue;
            }
        }
        long end = new Date().getTime();
        System.out.println("Make Result Time: " +  (end - start));
        return results;
    }

    private static String makeDescription(Document document, String query) {
        query = query.replaceAll("\\p{Punct}", " ");
        String[] tokens = query.split(" ");
        HashMap<Element, Integer> counter = new HashMap<>();
        for (String token: tokens) {
            Element element = document.select("p:containsOwn(" + token + ")").first();
            if (element != null) {
                counter.put(element, counter.getOrDefault(element, 0) + 1);
            }
        }
        String description = "";
        if (counter.isEmpty()){
            Elements elements = document.select("meta[name=description]");
            if (elements.isEmpty()) {
                elements = document.select("p");
                if (!elements.isEmpty()) {
                    for (Element element : elements) {
                        description = element.text();
                        if (description.length() != 0) {
                            break;
                        }
                    }
                }
            } else {
                description = elements.get(0).attr("content");
            }
        } else {
            Element maxKey = null;
            int maxVal = -1;
            for (Element key: counter.keySet()) {
                if (counter.get(key) > maxVal) {
                    maxKey = key;
                    maxVal = counter.get(key);
                }
            }
            assert maxKey != null;
            description = maxKey.text();
        }
        return description;
    }

    public static void main(String[] args) {
        int port = 45550;
        if (args.length != 0)
            port = Integer.parseInt(args[0]);
        System.out.println("Working on port: " + port);
        port(port);
        staticFiles.externalLocation("www");
        staticFileLocation("www");
        int maxThreads = 8;
        int minThreads = 2;
        int timeOutMillis = 30000;
        threadPool(maxThreads, minThreads, timeOutMillis);

        final int PAGE_SIZE = 6;

        System.out.println(Paths.get("./").toAbsolutePath().toString());

        options("/*", (request, response) -> {
            String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
            if (accessControlRequestHeaders != null) {
                response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
            }

            String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
            if (accessControlRequestMethod != null) {
                response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
            }

            return "OK";
        });

        before((request, response) -> response.header("Access-Control-Allow-Origin", "*"));

        get("/api/result", (req, res) -> {
            String query = req.queryParams("q");
            int page = Integer.parseInt(req.queryParams("p"));
            int left = (page - 1) * PAGE_SIZE;
            int right = page * PAGE_SIZE;
            List<String> urls = IndexerHelper.processQuery(query, left, right);
            int total = Integer.parseInt(urls.get(urls.size() - 1));
            if (urls.size() == 1) {
                String json = "{\"data\": []" + "," + "\"total\":" + total + "}";
                return json;
            }
            urls = urls.subList(0, urls.size() - 1);
            List<Result> results = makeResultFromUrl(urls, query);
            ObjectMapper objectMapper = new ObjectMapper();
            String data = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(results);
            String json = "{\"data\":" + data + "," + "\"total\":" + total + "}";
            return json;
        });

        // The method signature should be get(String url, Route route) where
        // Route's default function is handle(Request req, Response res)
        System.out.println("Waiting to handle requests!");

        // Generally this isn't necessary, but it blocks until the web server is initialized
        // The server should stay running in the background until Ctrl-C is hit
        awaitInitialization();
    }
}
