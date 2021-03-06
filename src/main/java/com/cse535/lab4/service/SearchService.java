package com.cse535.lab4.service;

import com.cse535.lab4.controller.Controller;
import com.cse535.lab4.model.*;
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
    private JSONObject weekTweetVolume;

    private static final Logger LOG = LoggerFactory.getLogger(SearchService.class);
    private static final String[] cities = {"nyc","paris","delhi", "bangkok", "mexico"};
    private static final String[] filterDates = {"[2018-10-01T20:00:00Z TO 2018-10-07T20:00:00Z]",
            "[2018-10-08T20:00:00Z TO 2018-10-14T20:00:00Z]",
            "[2018-10-15T20:00:00Z TO 2018-10-21T20:00:00Z]",
            "[2018-10-22T20:00:00Z TO 2018-10-28T20:00:00Z]",
            "[2018-10-29T20:00:00Z TO 2018-11-04T20:00:00Z]",
            "[2018-11-05T20:00:00Z TO 2018-11-11T20:00:00Z]",
            "[2018-11-12T20:00:00Z TO 2018-11-18T20:00:00Z]"};

    public TweetData getTweets(String search, String city, String lang, Integer start, int docs) {
        LOG.info("Fetching tweets from solr..");
        TweetData tweetData = new TweetData();
        String requestQuery = "";
        String langQuery = "";
        String searchQuery = "";
        String cityQuery;

        if(city != null) {
            cityQuery = "city:" + city;
            requestQuery = requestQuery + cityQuery;
        }

        if(search != null) {
            String[] strs = search.split(",");
            for (int i=0; i<strs.length; i++) {
                String s = strs[i];
                if(i>0)
                    searchQuery = searchQuery + " AND ";
                if(s.startsWith("\"")) {
                    searchQuery = searchQuery + s.substring(1,s.length()-1);
                } else if (s.startsWith("#")) {
                    searchQuery = searchQuery + "hashtags:" + s.substring(1,s.length());
                } else {
                    searchQuery = searchQuery + s;
                }
            }
            if(requestQuery.isEmpty())
                requestQuery = searchQuery;
            else
                requestQuery = requestQuery + " AND " + searchQuery;
        }

        if(lang != null) {
            langQuery = langQuery + "(";
            String [] languages = lang.split(",");
            for(int i=0; i<languages.length; i++) {
                if(i>0)
                    langQuery = langQuery + " OR ";
                langQuery = langQuery + "lang:" + languages[i];
            }
            langQuery = langQuery + ")";
            if(requestQuery.isEmpty())
                requestQuery = langQuery;
            else
                requestQuery = requestQuery + " AND " + langQuery;
        }
        if(requestQuery.isEmpty())
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

    public JSONObject getLanguageTweetCount(String language) {
        LOG.info("Fetching language-tweet count..");
        JSONObject languageData = new JSONObject();
        JSONArray languageObj = new JSONArray();
        if(language != null) {
            CompletableFuture<LanguageTweetData> data = getTweetCountForLangauage(language);
            CompletableFuture.completedFuture(data).join();
            try {
                languageObj.add(data.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error fetching tweet count for language " + language, e);
            }
        } else {
            CompletableFuture<LanguageTweetData> en = getTweetCountForLangauage("en");
            CompletableFuture<LanguageTweetData> es = getTweetCountForLangauage("es");
            CompletableFuture<LanguageTweetData> fr = getTweetCountForLangauage("fr");
            CompletableFuture<LanguageTweetData> hi = getTweetCountForLangauage("hi");
            CompletableFuture<LanguageTweetData> th =  getTweetCountForLangauage("th");
            CompletableFuture.allOf(en,es,fr,hi,th).join();
            try {
                languageObj.add(en.get());
                languageObj.add(es.get());
                languageObj.add(fr.get());
                languageObj.add(hi.get());
                languageObj.add(th.get());
            } catch (InterruptedException | ExecutionException e) {
                LOG.error("Error fetching tweet count for language " + language, e);
            }
        }
        languageData.put("languages",languageObj);
        LOG.info("Fetched language-tweet data!");
        return languageData;
    }

    @Async
    public CompletableFuture<LanguageTweetData> getTweetCountForLangauage(String language) {
        long tweetCount = 0;
        String cityQuery = "lang:" + language;
        LOG.debug("Fetching tweet count from solr for " + language);
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
            LOG.info("Fetched tweet count from solr for " + language);
        } catch (SolrServerException | IOException e) {
            LOG.error("Error fetching data from solr", e);
        }
        LanguageTweetData data = new LanguageTweetData();
        data.setCount(tweetCount);
        data.setLanguage(language);
        return CompletableFuture.completedFuture(data);
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

    private ArrayList<JSONObject> parseTweets(SolrDocumentList results) {
        ArrayList<JSONObject> tweets = new ArrayList<>();
        for(SolrDocument doc : results) {
            JSONObject tweet = new JSONObject();
            tweet.put("id",(String) doc.get("id"));
            tweet.put("city",doc.get("city"));
            ArrayList<String> data = (ArrayList<String>) doc.get("text");
            tweet.put("summary",data.get(0));
            data = (ArrayList<String>) doc.get("user.profile_image_url");
            if (data != null) {
                tweet.put("user.profile_image_url",data.get(0));
            } else {
                tweet.put("user.profile_image_url","");
            }
            data = (ArrayList<String>) doc.get("user.entities.url.urls.expanded_url");
            if (data != null) {
                tweet.put("user.entities.url.urls.expanded_url",data.get(0));
            } else {
                tweet.put("user.entities.url.urls.expanded_url","");
            }
            data = (ArrayList<String>) doc.get("user.followers_count");
            if (data != null) {
                tweet.put("user.followers_count",data.get(0));
            } else {
                tweet.put("user.followers_count","");
            }
            data = (ArrayList<String>) doc.get("user.friends_count");
            if (data != null) {
                tweet.put("user.friends_count",data.get(0));
            } else {
                tweet.put("user.friends_count", "");
            }
            data = (ArrayList<String>) doc.get("user.location");
            if (data != null) {
                tweet.put("user.location",data.get(0));
            } else {
                tweet.put("user.location", "");
            }
            data = (ArrayList<String>) doc.get("user.screen_name");
            if (data != null) {
                tweet.put("user.screen_name",data.get(0));
            } else {
                tweet.put("user.screen_name", "");
            }
            data = (ArrayList<String>) doc.get("user.name");
            if (data != null) {
                tweet.put("user.name",data.get(0));
            } else {
                tweet.put("user.name", "");
            }
            data = (ArrayList<String>) doc.get("sentiment");
            if (data != null) {
                tweet.put("sentiment",data.get(0));
            } else {
                tweet.put("sentiment", "");
            }
            tweets.add(tweet);
        }
        return tweets;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        hashtags = getHashtagsList();
        weekTweetVolume = getWeekTweetVolumeData();
    }

    private JSONObject getWeekTweetVolumeData() {
        LOG.info("Fetching weekly tweet volume data from solr..");
        JSONObject weekTweetVolumeObject = new JSONObject();
        ArrayList<WeekTweetVolume> weekTweetVolume = new ArrayList<>();
        JSONArray weeklyTweetVolumeData = new JSONArray();
        CompletableFuture<WeekTweetVolume> week1 = getWeekTweetVolumeDataForWeek(filterDates[0], env.getProperty("startDateWeek1"), env.getProperty("endDateWeek1"));
        CompletableFuture<WeekTweetVolume> week2 = getWeekTweetVolumeDataForWeek(filterDates[1], env.getProperty("startDateWeek2"), env.getProperty("endDateWeek2"));
        CompletableFuture<WeekTweetVolume> week3 = getWeekTweetVolumeDataForWeek(filterDates[2], env.getProperty("startDateWeek3"), env.getProperty("endDateWeek3"));
        CompletableFuture<WeekTweetVolume> week4 = getWeekTweetVolumeDataForWeek(filterDates[3], env.getProperty("startDateWeek4"), env.getProperty("endDateWeek4"));
        CompletableFuture<WeekTweetVolume> week5 = getWeekTweetVolumeDataForWeek(filterDates[4], env.getProperty("startDateWeek5"), env.getProperty("endDateWeek5"));
        CompletableFuture<WeekTweetVolume> week6 = getWeekTweetVolumeDataForWeek(filterDates[5], env.getProperty("startDateWeek6"), env.getProperty("endDateWeek6"));
        CompletableFuture<WeekTweetVolume> week7 = getWeekTweetVolumeDataForWeek(filterDates[6], env.getProperty("startDateWeek7"), env.getProperty("endDateWeek7"));
        CompletableFuture.allOf(week1,week2,week3,week4,week5,week6,week7).join();
        try {
            weeklyTweetVolumeData.add(week1.get());
            weeklyTweetVolumeData.add(week2.get());
            weeklyTweetVolumeData.add(week3.get());
            weeklyTweetVolumeData.add(week4.get());
            weeklyTweetVolumeData.add(week5.get());
            weeklyTweetVolumeData.add(week6.get());
            weeklyTweetVolumeData.add(week7.get());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error fetching weekly tweet volume data.. ");
        }
        weekTweetVolumeObject.put("tweetVolumeData",weeklyTweetVolumeData);
        LOG.info("Fetched weekly tweet volume data!");
        return weekTweetVolumeObject;
    }

    @Async
    public CompletableFuture<WeekTweetVolume> getWeekTweetVolumeDataForWeek(String dateFilter, String startDate, String endDate) {
        long tweetCount = 0;
        String weekQuery = "tweet_date:" + dateFilter;
        LOG.debug("Fetching tweet count from solr for week: " + dateFilter );
        SolrClient solrClient = controller.getSolrClient();
        SolrQuery query = new SolrQuery();
        query.set("q", "*:*");
        query.set("rows", "1");
        query.setStart(0);
        query.set("fq", weekQuery);
        LOG.debug("Query: " + query.getQuery());
        try {
            QueryResponse response = solrClient.query(query);
            tweetCount = response.getResults().getNumFound();
            LOG.debug("Tweet count: " + tweetCount);
            LOG.info("Fetched tweet count for week " + dateFilter);
        } catch (SolrServerException | IOException e) {
            LOG.error("Error fetching data from solr", e);
        }
        WeekTweetVolume weekVolumeObj = new WeekTweetVolume();
        weekVolumeObj.setCount(tweetCount);
        weekVolumeObj.setStartDate(startDate);
        weekVolumeObj.setEndDate(endDate);
        return CompletableFuture.completedFuture(weekVolumeObj);
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

    public JSONObject getWeeklyTweetVolumeData() {
        return weekTweetVolume;
    }

    @Async
    public CompletableFuture<CityTopicWeekSentiment> getCityTopicSentimentDataForWeek(String dateFilter, String startDate, String endDate, String city, String topic) {
        long positiveCount = 0;
        long negativeCount = 0;
        long neutralCount = 0;
        String weekQuery = "tweet_date:" + dateFilter;
        LOG.debug("Fetching city topic sentiment data from solr for week: " + dateFilter );
        SolrClient solrClient = controller.getSolrClient();
        SolrQuery query = new SolrQuery();
        //query.set("q", "AND city:" + city + " AND topic:" + topic);
        query.set("q", "topic:" + topic + " AND city:" + city);
        query.set("rows", "1");
        query.setStart(0);
        query.set("fq", weekQuery);
        query.setFacet(true);
        String positivefacetQuery = "sentiment:positive";
        String negativefacetQuery = "sentiment:negative";
        String neutralfacetQuery = "sentiment:neutral";
        query.addFacetQuery(positivefacetQuery);
        query.addFacetQuery(negativefacetQuery);
        query.addFacetQuery(neutralfacetQuery);
        LOG.debug("Query: " + query.getQuery());
        try {
            QueryResponse response = solrClient.query(query);
            positiveCount = response.getFacetQuery().get("sentiment:positive");
            negativeCount = response.getFacetQuery().get("sentiment:negative");
            neutralCount = response.getFacetQuery().get("sentiment:neutral");
            LOG.debug("Positive: " + positiveCount + "Negative: " + negativeCount + "Neutral: " + neutralCount);
            LOG.info("Fetched tweet count for week " + dateFilter);
        } catch (SolrServerException | IOException e) {
            LOG.error("Error fetching data from solr", e);
        }
        CityTopicWeekSentiment cityTopicWeekSentimentObject = new CityTopicWeekSentiment();
        cityTopicWeekSentimentObject.setPositiveCount(positiveCount);
        cityTopicWeekSentimentObject.setNegativeCount(negativeCount);
        cityTopicWeekSentimentObject.setNeutralCount(neutralCount);
        cityTopicWeekSentimentObject.setStartDate(startDate);
        cityTopicWeekSentimentObject.setEndDate(endDate);
        return CompletableFuture.completedFuture(cityTopicWeekSentimentObject);
    }

    public ArrayList<CityTopicWeekSentiment> getCityTopicWeeklySentiments(String city, String topic) {
        ArrayList<CityTopicWeekSentiment> cityTopicWeekSentiments = new ArrayList<>();

        LOG.info("Fetching city topic weekly sentiment data from solr..");

        CompletableFuture<CityTopicWeekSentiment> week1 = getCityTopicSentimentDataForWeek(filterDates[0], env.getProperty("startDateWeek1"), env.getProperty("endDateWeek1"), city, topic);
        CompletableFuture<CityTopicWeekSentiment> week2 = getCityTopicSentimentDataForWeek(filterDates[1], env.getProperty("startDateWeek2"), env.getProperty("endDateWeek2"), city, topic);
        CompletableFuture<CityTopicWeekSentiment> week3 = getCityTopicSentimentDataForWeek(filterDates[2], env.getProperty("startDateWeek3"), env.getProperty("endDateWeek3"), city, topic);
        CompletableFuture<CityTopicWeekSentiment> week4 = getCityTopicSentimentDataForWeek(filterDates[3], env.getProperty("startDateWeek4"), env.getProperty("endDateWeek4"), city, topic);
        CompletableFuture<CityTopicWeekSentiment> week5 = getCityTopicSentimentDataForWeek(filterDates[4], env.getProperty("startDateWeek5"), env.getProperty("endDateWeek5"), city, topic);
        CompletableFuture<CityTopicWeekSentiment> week6 = getCityTopicSentimentDataForWeek(filterDates[5], env.getProperty("startDateWeek6"), env.getProperty("endDateWeek6"), city, topic);
        CompletableFuture<CityTopicWeekSentiment> week7 = getCityTopicSentimentDataForWeek(filterDates[6], env.getProperty("startDateWeek7"), env.getProperty("endDateWeek7"), city, topic);
        CompletableFuture.allOf(week1,week2,week3,week4,week5,week6,week7).join();
        try {
            cityTopicWeekSentiments.add(week1.get());
            cityTopicWeekSentiments.add(week2.get());
            cityTopicWeekSentiments.add(week3.get());
            cityTopicWeekSentiments.add(week4.get());
            cityTopicWeekSentiments.add(week5.get());
            cityTopicWeekSentiments.add(week6.get());
            cityTopicWeekSentiments.add(week7.get());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error fetching weekly city topic sentiment data.. ");
        }
        LOG.info("Fetched weekly city topic sentiment data!");

        return  cityTopicWeekSentiments;
    }

    @Async
    public CompletableFuture<CitySentiment> getSearchBasedSentimentsForCity(String city, String search) {
        CitySentiment citySentiment = new CitySentiment();
        long positiveCount;
        long negativeCount;
        long neutralCount;
        String searchQuery = "";

        if(search != null) {
            String[] strs = search.split(",");
            for (int i=0; i<strs.length; i++) {
                String s = strs[i];
                if(i>0)
                    searchQuery = searchQuery + " AND ";
                if(s.startsWith("\"")) {
                    searchQuery = searchQuery + s.substring(1,s.length()-1);
                } else if (s.startsWith("#")) {
                    searchQuery = searchQuery + "hashtags:" + s.substring(1,s.length());
                } else {
                    searchQuery = searchQuery + s;
                }
            }
        }

        if(searchQuery.isEmpty()) {
            searchQuery = "*:* AND city:" + city;
        } else {
            searchQuery = searchQuery + " AND city:" + city;
        }

        SolrClient solrClient = controller.getSolrClient();
        SolrQuery query = new SolrQuery();
        query.set("q", searchQuery);
        query.set("rows", 1);
        query.setStart(0);
        query.setFacet(true);
        String positivefacetQuery = "sentiment:positive";
        String negativefacetQuery = "sentiment:negative";
        String neutralfacetQuery = "sentiment:neutral";
        query.addFacetQuery(positivefacetQuery);
        query.addFacetQuery(negativefacetQuery);
        query.addFacetQuery(neutralfacetQuery);
        LOG.debug("Query: " + query.getQuery());
        try {
            QueryResponse response = solrClient.query(query);
            if(response != null) {
                positiveCount = response.getFacetQuery().get("sentiment:positive");
                negativeCount = response.getFacetQuery().get("sentiment:negative");
                neutralCount = response.getFacetQuery().get("sentiment:neutral");
                LOG.debug("Positive: " + positiveCount + "Negative: " + negativeCount + "Neutral: " + neutralCount);
                citySentiment.setCity(city);
                citySentiment.setPositiveCount(positiveCount);
                citySentiment.setNegativeCount(negativeCount);
                citySentiment.setNeutralCount(neutralCount);
                LOG.info("Fetched search based sentiments for city from solr!");
            } else {
                LOG.info("No search result from solr for query: " + query.getQuery());
            }
        } catch (SolrServerException | IOException e) {
            LOG.error("Error fetching/searching tweets on solr", e);
        }
        return CompletableFuture.completedFuture(citySentiment);
    }

    public ArrayList<CitySentiment> getCityWiseSearchTopicSentimentData(String search) {
        LOG.info("Fetching city wise sentiment data for search topic..");
        ArrayList<CitySentiment> citySentiments = new ArrayList<>();

        CompletableFuture<CitySentiment> nyc = getSearchBasedSentimentsForCity(cities[0], search);
        CompletableFuture<CitySentiment> paris = getSearchBasedSentimentsForCity(cities[1], search);
        CompletableFuture<CitySentiment> delhi = getSearchBasedSentimentsForCity(cities[2], search);
        CompletableFuture<CitySentiment> bangkok = getSearchBasedSentimentsForCity(cities[3], search);
        CompletableFuture<CitySentiment> mexico = getSearchBasedSentimentsForCity(cities[4], search);
        CompletableFuture.allOf(nyc,paris,delhi,bangkok,mexico).join();

        try {
            citySentiments.add(nyc.get());
            citySentiments.add(paris.get());
            citySentiments.add(delhi.get());
            citySentiments.add(bangkok.get());
            citySentiments.add(mexico.get());
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Error fetching search based city sentiment data.. ");
        }
        LOG.info("Fetched search based sentiments for city from solr!");
        return citySentiments;
    }
}
