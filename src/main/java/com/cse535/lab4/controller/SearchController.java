package com.cse535.lab4.controller;

import com.cse535.lab4.model.Hashtag;
import com.cse535.lab4.model.TweetData;
import com.cse535.lab4.service.SearchService;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;

@RestController
public class SearchController {

    @Autowired
    private SearchService searchService;

    private static final Logger LOG = LoggerFactory.getLogger(SearchController.class);

    /**
     * API- http://localhost:5000/tweets/cities/count?city=NYC
     * @param city non-mandatory query param, if not provided- perform search on all 5 cities
     * @return
     */
    @GetMapping(value = {"/tweets/cities/count"})
    public JSONObject getCityTweetCount(@RequestParam(value = "city", required = false) String city) {
        LOG.info("Fetching city-tweet count..");
        return searchService.getCityTweetCount(city);
    }

    /**
     * API- http://localhost:5000/tweets/languages/count?lang=en
     * @param language non-mandatory query param, if not provided- perform search on all 5 languages
     * @return
     */
    @GetMapping(value = {"/tweets/languages/count"})
    public JSONObject getLanguageTweetCount(@RequestParam(value = "lang", required = false) String language) {
        LOG.info("Fetching lang-tweet count..");
        return searchService.getLanguageTweetCount(language);
    }

    /**
     * API- http://localhost:5000/tweets?city=NYC,Delhi&lang=en,es&docs=10&start=0
     * API honors provided params else perform a wild search
     * @param city non-mandatory param, if not provided, perform a wild search "*:*"
     * @param lang non-mandatory param, if not provided, perform a wild search "*:*"
     * @param start non-mandatory param, default- 0
     * @param docs non-mandatory param, default- 10
     * @return
     */
    @GetMapping(value = {"/tweets/list"})
    public TweetData getTweets(@RequestParam(value = "city", required = false) String city, @RequestParam(value = "lang", required = false) String lang, @RequestParam(value = "start", required = false, defaultValue = "0") Integer start, @RequestParam(value = "docs", required = false, defaultValue = "10") int docs) {
        LOG.info("Fetching tweets..");
        return searchService.getTweets(city,lang,start,docs);
    }

    /**
     * API- http://localhost:5000/tweets/hashtags/list
     * @return list of top 20 hashtags
     */
    @GetMapping(value = {"/tweets/hashtags/list"})
    public ArrayList<Hashtag> getHashtags() {
        LOG.info("Fetching hashtags..");
        return searchService.getHashtags();
    }

    @GetMapping(value = {"/tweets/weeklyVolume"})
    public JSONObject getweeklyVolumeData() {
        LOG.info("Fetching weekly volume data..");
        return searchService.getWeeklyTweetVolumeData();
    }


}
