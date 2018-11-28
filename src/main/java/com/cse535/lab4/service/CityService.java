package com.cse535.lab4.service;

import com.cse535.lab4.controller.Controller;
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
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class CityService {

    @Autowired
    private Controller controller;

    @Autowired
    private Environment env;

    private static final Logger LOG = LoggerFactory.getLogger(CityService.class);
    private static final String[] cities = {"nyc","paris","delhi", "bangkok", "mexico"};

    public JSONObject getCityTweetCount(String city) {
        LOG.info("Fetching city-tweet count..");
        JSONObject cityTweets = new JSONObject();
        JSONArray citiyData = new JSONArray();
        if(city != null) {
            citiyData.add(getCityData(city));
        }
        else {
            for(String s : cities) {
                citiyData.add(getCityData(s));
            }
        }
        cityTweets.put("cities",citiyData);
        LOG.info("Fetched city-tweet data!");
        return cityTweets;
    }

    private JSONObject getCityData(String city) {
        JSONObject cityObj = new JSONObject();
        cityObj.put("city",city);
        cityObj.put("country",env.getProperty(city));
        cityObj.put("tweetCount",getTweetCountForCity(city));
        LOG.debug("City data: " + cityObj.toJSONString());
        return cityObj;
    }

    private int getTweetCountForCity(String city) {
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
        return list.size();
    }
}
