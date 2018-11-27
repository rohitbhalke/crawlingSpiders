package com.cse535.lab4.controller;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class Controller implements InitializingBean {

    @Autowired
    private Environment env;

    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);

    public SolrClient getSolrClient() {
        return solrClient;
    }

    private SolrClient solrClient = null;

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    public void init() {
        LOG.info("Initializing solr client..");
        String urlString = "http://" + env.getProperty("solr.host") + ":" + env.getProperty("solr.port") + "/solr/" + env.getProperty("solr.core");
        LOG.debug("Solr connecting to: " + urlString);
        solrClient = new HttpSolrClient.Builder(urlString).build();
        LOG.info("Solr client successfully initialized!");
    }

}
