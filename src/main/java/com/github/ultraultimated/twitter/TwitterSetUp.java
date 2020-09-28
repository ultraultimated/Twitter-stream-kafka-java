package com.github.ultraultimated.twitter;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterSetUp implements TwitterProperties {
    public TwitterSetUp() {
    }

    public static void main(String[] args) {
        new TwitterSetUp().run();
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(TwitterSetUp.class);
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //create twitter client
        Client client = createTwitterClient(msgQueue, logger);
        client.connect();

        // Dump data to console
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(2, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.error("Interrupted Exception Occurred", e);
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
            }
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue, Logger logger) {
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("Manchester ");
        endpoint.trackTerms(terms);
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();

    }

}
