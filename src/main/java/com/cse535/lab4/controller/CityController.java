package com.cse535.lab4.controller;

import com.cse535.lab4.service.CityService;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CityController {

    @Autowired
    private CityService cityService;

    private static final Logger LOG = LoggerFactory.getLogger(CityController.class);

    @GetMapping(value = {"/tweets/count"})
    public JSONObject getCityTweetCount(@RequestParam(value = "city", required = false) String city) {
        LOG.info("Fetching city-tweet data..");
        return cityService.getCityTweetCount(city);
    }


}
