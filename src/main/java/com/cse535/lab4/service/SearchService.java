package com.cse535.lab4.service;

import com.cse535.lab4.controller.Controller;
import com.cse535.lab4.model.Hashtag;
import com.cse535.lab4.model.Tweet;
import com.cse535.lab4.model.TweetCountData;
import com.cse535.lab4.model.TweetData;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class SearchService implements InitializingBean {
    @Autowired
    private Controller controller;
    @Autowired
    private Environment env;

    private ArrayList<Hashtag> hashtags;

    private static final Logger LOG = LoggerFactory.getLogger(SearchService.class);
    private static final String[] cities = {"nyc","paris","delhi", "bangkok", "mexico"};

    public TweetData getTweets(String city, String lang, Integer start, int docs) {
        LOG.info("Fetching tweets from solr..");
        TweetData tweetData = new TweetData();
        String requestQuery;
        String langQuery = "";
        if(lang != null) {
            langQuery = langQuery + "(";
            String [] languages = lang.split(",");
            for(int i=0; i<languages.length; i++) {
                if(i>0)
                    langQuery = langQuery + " OR ";
                langQuery = langQuery + "lang:" + languages[i];
            }
            langQuery = langQuery + ")";
        }
        if(city != null && !langQuery.isEmpty())
            requestQuery = "city:" + city + " AND " + langQuery;
        else if(city == null && !langQuery.isEmpty())
            requestQuery = langQuery;
        else if(lang == null && city != null)
            requestQuery = "city:" + city;
        else
            requestQuery = "*:*";
        SolrClient solrClient = controller.getSolrClient();
        SolrQuery query = new SolrQuery();
        query.set("q", requestQuery);
        query.set("rows", docs);
        query.setStart(start);
        LOG.debug("Query: " + query.getQuery());
        try {
            QueryResponse response = solrClient.query(query);
            if(response != null) {
                tweetData.setStart(start);
                tweetData.setTotal(response.getResults().getNumFound());
                tweetData.setTweets(parseTweets(response.getResults()));
                LOG.info("Fetched tweets from solr!");
            } else {
                LOG.info("No search result from solr for query: " + query.getQuery());
            }
        } catch (SolrServerException | IOException e) {
            LOG.error("Error fetching/searching tweets on solr", e);
        }
        return tweetData;
    }

    public JSONObject getCityTweetCount(String city) {
        LOG.info("Fetching city-tweet count..");
        JSONObject cityTweets = new JSONObject();
        JSONArray citiyData = new JSONArray();
        if(city != null) {
            CompletableFuture<TweetCountData> data = getTweetCountForCity(city);
            CompletableFuture.completedFuture(data).join();
            try {
                citiyData.add(data.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error fetching tweet count for city " + city, e);
            }
        } else {
            CompletableFuture<TweetCountData> nyc = getTweetCountForCity(cities[0]);
            CompletableFuture<TweetCountData> paris = getTweetCountForCity(cities[1]);
            CompletableFuture<TweetCountData> delhi = getTweetCountForCity(cities[2]);
            CompletableFuture<TweetCountData> bangkok = getTweetCountForCity(cities[3]);
            CompletableFuture<TweetCountData> mexico = getTweetCountForCity(cities[4]);
            CompletableFuture.allOf(nyc,paris,delhi,bangkok,mexico).join();
            try {
                citiyData.add(nyc.get());
                citiyData.add(paris.get());
                citiyData.add(delhi.get());
                citiyData.add(bangkok.get());
                citiyData.add(mexico.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error fetching tweet count for city " + city, e);
            }
        }
        cityTweets.put("cities",citiyData);
        LOG.info("Fetched city-tweet data!");
        return cityTweets;
    }

    @Async
    public CompletableFuture<TweetCountData> getTweetCountForCity(String city) {
        long tweetCount = 0;
        String cityQuery = "city:" + city;
        LOG.debug("Fetching tweet count from solr for " + city);
        SolrClient solrClient = controller.getSolrClient();
        SolrQuery query = new SolrQuery();
        query.set("q", cityQuery);
        query.set("rows", "1");
        query.setStart(0);
        LOG.debug("Query: " + query.getQuery());
        try {
            QueryResponse response = solrClient.query(query);
            tweetCount = response.getResults().getNumFound();
            LOG.debug("Tweet count: " + tweetCount);
            LOG.info("Fetched tweet count from solr for " + city);
        } catch (SolrServerException | IOException e) {
            LOG.error("Error fetching data from solr", e);
        }
        TweetCountData cityObj = new TweetCountData();
        cityObj.setCity(city);
        cityObj.setCountry(env.getProperty(city.toLowerCase()));
        cityObj.setTweetCount(tweetCount);
        return CompletableFuture.completedFuture(cityObj);
    }

    private ArrayList<Tweet> parseTweets(SolrDocumentList results) {
        ArrayList<Tweet> tweets = new ArrayList<>();
        for(SolrDocument doc : results) {
            Tweet tweet = new Tweet();
            tweet.setId((String) doc.get("id"));
            ArrayList<String> data = (ArrayList<String>) doc.get("text");
            tweet.setSummary(data.get(0));
            data = (ArrayList<String>) doc.get("user.profile_image_url");
            if (data != null) {
                tweet.setProfileURL(data.get(0));
            } else {
                tweet.setProfileURL("");
            }
            data = (ArrayList<String>) doc.get("entities.urls.url");
            if (data != null) {
                tweet.setTwitterURL(data.get(0));
            } else {
                tweet.setTwitterURL("");
            }
            data = (ArrayList<String>) doc.get("lang");
            if (data != null) {
                tweet.setLanguage(data.get(0));
            } else {
                tweet.setLanguage("");
            }
            tweets.add(tweet);
        }
        return tweets;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        hashtags = getHashtagsList();
    }

    private ArrayList<Hashtag> getHashtagsList() {
        ArrayList<Hashtag> hashtags = new ArrayList<>();
        LOG.debug("Fetching hashtags from solr..");
        SolrClient solrClient = controller.getSolrClient();
        SolrQuery query = new SolrQuery();
        query.set("q", "*:*");
        query.set("rows", "0");
        query.setStart(0);
        query.setFacet(true);
        query.addFacetField("hashtags");
        LOG.debug("Query: " + query.getQuery());
        try {
            //response.getFacetFields().get(0)._values
            QueryResponse response = solrClient.query(query);
            List<FacetField.Count> tags = response.getFacetFields().get(0).getValues();
            for(int i=0; i<20; i++) {
                Hashtag tag = new Hashtag();
                FacetField.Count cTag = tags.get(i);
                tag.setName(cTag.getName());
                tag.setCount(cTag.getCount());
                hashtags.add(tag);
            }
            LOG.info("Fetched hashtags list from solr");
        } catch (SolrServerException | IOException e) {
            LOG.error("Error fetching data from solr", e);
        }
        return hashtags;
    }

    public ArrayList<Hashtag> getHashtags() {
        return hashtags;
    }
}
