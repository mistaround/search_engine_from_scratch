package edu.upenn.cis.crawler;

import java.util.ArrayList;

public class RuleItem {
	
	private String userAgent = null;
	private ArrayList<String> disallow = new ArrayList<>();
	private ArrayList<String> allow = new ArrayList<>();
	private int delay = 0;
	
	RuleItem(){}

	public String getUserAgent() {
		return userAgent;
	}

	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}

	public ArrayList<String> getDisallow() {
		return disallow;
	}

	public void addDisallow(String location) {
		this.disallow.add(location);
	}

	public ArrayList<String> getAllow() {
		return allow;
	}

	public void addAllow(String location) {
		this.allow.add(location);
	}
	

	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}
	



}
