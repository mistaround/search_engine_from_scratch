package edu.upenn.cis.crawler;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.net.ssl.HttpsURLConnection;

import edu.upenn.cis.crawler.utils.URLInfo;

public class RobotsHandler {
	static String path = "";
	public static AtomicInteger delay = new AtomicInteger(0);
	
	public static RuleItem getTheRule(String url, String agent, String hostname) {
		RuleItem rule = null;
		HashMap<String, RuleItem> ruleMap = getRuleMap(url, hostname);
		if(ruleMap==null) {
			return rule;
		}
		rule = findProperRuleItem(agent, ruleMap);
		setDelay(rule!=null?rule.getDelay():0);
		return rule;
	}
	public static boolean handleRobotFile(RuleItem rule) {
		if(rule==null) { return true; }
		return findRelatedSetting(rule, path);
	}
	
	public static HashMap<String, RuleItem> getRuleMap(String url, String hostname){
		String domain = url.startsWith("https")?"https://"+hostname+"/":"http://"+hostname+"/";
		String root = domain+"robots.txt";
		path = new URLInfo(url).getFilePath();
		String result = null;
		HashMap<String, RuleItem> ruleMap = new HashMap<>();
		RuleItem rule = null;
		InputStream inputStream = null;
		try { 
			URL urlObj = new URL(root);
			if(root.startsWith("https")) {
				HttpsURLConnection httpsCon = (HttpsURLConnection) urlObj.openConnection();
				httpsCon.connect();
				inputStream = httpsCon.getInputStream();
				result = convertInputStreamToString(inputStream);
				httpsCon.disconnect();
			}else {
				HttpURLConnection httpCon = (HttpURLConnection) urlObj.openConnection();
				httpCon.connect();
				inputStream= httpCon.getInputStream();
				result = convertInputStreamToString(inputStream);
				httpCon.disconnect();
			}
			String[] ansList = result.split("\n");
			for(String item: ansList) {
				if(item.toLowerCase().startsWith("user-agent")) {
					if(rule==null) { rule = new RuleItem(); }
					rule.setUserAgent(item.split(":")[1].strip());
				}else if(item.toLowerCase().startsWith("disallow")&&rule!=null) {
//					rule.addDisallow(item.split(":")[1].strip());
				}else if(item.toLowerCase().startsWith("allow")&&rule!=null) {
//					rule.addAllow(item.split(":")[1].strip());
				}else if(item.toLowerCase().startsWith("crawl-delay")&&rule!=null) {
					String time = item.split(":")[1].strip().split(" ")[0];
					if(time.contains(".")) {
						rule.setDelay(Integer.parseInt((String) time.subSequence(0, time.indexOf(".")))+1);
					}else {
						rule.setDelay(Integer.parseInt(time));
					}
				}else {
					if(rule!=null) {
						ruleMap.put(rule.getUserAgent(), rule);
					}
					rule = null;
				}
			}
		} catch (MalformedURLException e) {
			System.out.println("url is invalid");
		} catch(FileNotFoundException e) {
			System.out.println("robots is not found");
		} catch (IOException e) {
			System.out.println("get data from connection has error.");
		}
		return ruleMap;
	}

	public static void setDelay(int delay) {
		RobotsHandler.delay.set(delay);;
	}

//	public static int getDelayTime(domainAccessTable domainTable, String url, String agent, String hostname) {
//		return domainTable.getDelayTime(hostname);
//	}
//	
//	public static long getLastAccessTime(domainAccessTable domainTable, String hostname) {
//		return domainTable.findDomain(hostname)!=null?domainTable.findDomain(hostname).getLastAccessTime():0;
//	}
	
	public static RuleItem findProperRuleItem(String agent, HashMap<String, RuleItem> ruleMap) {
		if(ruleMap.get(agent)!=null) {
			return ruleMap.get(agent);
		}
		return ruleMap.get("*")!=null?ruleMap.get("*"):null;
	}
	
	public static boolean findRelatedSetting(RuleItem rule, String url) {
		boolean flag = true;
		for(String location: rule.getDisallow()) {
			if(location.equals(url)||url.startsWith(location)) {
				flag = false;
			}
		}
		for(String location: rule.getAllow()) {
			if(location.equals(url)||url.startsWith(location)) {
				flag = true;
			}
		}
		return flag;
	}

	public static int getDelay() {
		return delay.get();
	}
	
	private static String convertInputStreamToString(InputStream inputStream) {
		String text = new BufferedReader(
			      new InputStreamReader(inputStream, StandardCharsets.UTF_8))
			        .lines()
			        .collect(Collectors.joining("\n"));
		return text;
	}
}
