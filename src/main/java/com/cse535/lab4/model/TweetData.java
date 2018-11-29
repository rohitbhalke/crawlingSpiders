package com.cse535.lab4.model;

import org.apache.solr.common.SolrDocumentList;

public class TweetData {
    long total;
    long start;

    public SolrDocumentList getTweets() {
        return tweets;
    }

    public void setTweets(SolrDocumentList tweets) {
        this.tweets = tweets;
    }

    SolrDocumentList tweets;


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
