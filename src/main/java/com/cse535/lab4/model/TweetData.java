package com.cse535.lab4.model;

import org.json.simple.JSONObject;

import java.util.ArrayList;

public class TweetData {
    long total;
    long start;
    ArrayList<JSONObject> tweets;

    public ArrayList<JSONObject> getTweets() {
        return tweets;
    }

    public void setTweets(ArrayList<JSONObject> tweets) {
        this.tweets = tweets;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }


}
