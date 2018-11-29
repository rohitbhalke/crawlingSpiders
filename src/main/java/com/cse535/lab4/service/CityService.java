package com.cse535.lab4.service;

import com.cse535.lab4.controller.Controller;
import com.cse535.lab4.model.CityTweetCount;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class CityService {

    @Autowired
    private Controller controller;

    @Autowired
    private Environment env;

    private static final Logger LOG = LoggerFactory.getLogger(CityService.class);
    private static final String[] cities = {"NYC","Paris","Delhi", "Bangkok", "Mexico"};

    public JSONObject getCityTweetCount(String city) {
        LOG.info("Fetching city-tweet count..");
        JSONObject cityTweets = new JSONObject();
        JSONArray citiyData = new JSONArray();
        if(city != null) {
            CompletableFuture<CityTweetCount> data = getTweetCountForCity(city);
            CompletableFuture.completedFuture(data).join();
            try {
                citiyData.add(data.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error fetching tweet count for city " + city, e);
            }
        }
        else {
            CompletableFuture<CityTweetCount> nyc = getTweetCountForCity(cities[0]);
            CompletableFuture<CityTweetCount> paris = getTweetCountForCity(cities[1]);
            CompletableFuture<CityTweetCount> delhi = getTweetCountForCity(cities[2]);
            CompletableFuture<CityTweetCount> bangkok = getTweetCountForCity(cities[3]);
            CompletableFuture<CityTweetCount> mexico = getTweetCountForCity(cities[4]);

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
    public CompletableFuture<CityTweetCount> getTweetCountForCity(String city) {
        SolrDocumentList list = new SolrDocumentList();
        String cityQuery = "city:" + city;
        LOG.debug("Fetching data from solr..");
        SolrClient solrClient = controller.getSolrClient();
        SolrQuery query = new SolrQuery();
        query.set("q", cityQuery);
        query.set("rows", "1000000");
        LOG.debug("Query: " + query.getQuery());
        try {
            QueryResponse response = solrClient.query(query);
            list = response.getResults();
            LOG.debug("Results: " + list.size());
            LOG.info("Fetched data from solr!");
        } catch (SolrServerException | IOException e) {
            LOG.error("Error fetching data from solr", e);
        }

        CityTweetCount cityObj = new CityTweetCount();
        cityObj.setCity(city);
        cityObj.setCountry(env.getProperty(city));
        cityObj.setTweetCount(list.size());
        return CompletableFuture.completedFuture(cityObj);
    }
}
