package com.cse535.lab4.controller;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
public class SolrController {

    @Autowired
    private Controller controller;

    private static final Logger LOG = LoggerFactory.getLogger(SolrController.class);

    @GetMapping(value = {"/tweets/count"})
    public void getTweetCount(@RequestParam("city") String city) {
        String cityQuery = "city:" + city;
        SolrClient solrClient = controller.getSolrClient();
        SolrQuery query = new SolrQuery();
        query.set("q", cityQuery);
        query.set("rows", "1000");
        try {
            QueryResponse response = solrClient.query(query);
            SolrDocumentList list = response.getResults();
            LOG.info("Results: " + list.size());
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
