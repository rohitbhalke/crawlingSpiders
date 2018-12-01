package com.cse535.lab4.controller;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
        String solrURL = "http://%s:%s/solr/%s";
        String urlString;
        boolean isLocal = false;
        InetAddress localhost;
        try {
            localhost = InetAddress.getLocalHost();
            isLocal = localhost.isSiteLocalAddress();
        } catch (UnknownHostException e) {
            LOG.error("Error reading solr config",e);
        }
        if(isLocal)
            urlString = String.format(solrURL,env.getProperty("solr.local.host"),env.getProperty("solr.local.port"),env.getProperty("solr.local.core"));
        else
            urlString = String.format(solrURL,env.getProperty("solr.remote.host"),env.getProperty("solr.remote.port"),env.getProperty("solr.remote.core"));
        LOG.debug("Solr connecting to: " + urlString);
        solrClient = new HttpSolrClient.Builder(urlString).build();
        LOG.info("Solr client successfully initialized!");
    }

}
