package com.practice.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.www.http.HttpClient;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Velmurugan Moorthy
 * 31.07.2019
 * This class would be invoked during the Twitter Kafka producer demo
 */
public class TwitterKafkaProducer {

    Logger loggerObject = LoggerFactory.getLogger(TwitterKafkaProducer.class.getName());
    List<String> terms = Lists.newArrayList("politics", "india", "TN" ,"Tamilnadu");

    public static void main(String[] args) {
     new TwitterKafkaProducer().invokeTwitterKafkaProducer();
    }

    public void invokeTwitterKafkaProducer() {
        loggerObject.info("Beginning of the application");
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<String>(1000);

        // Create Twitter client
        Client twitterClient = createTwitterClient(messageQueue);
        twitterClient.connect();

        // Create Kafka producer
        //KafkaProducer<String,String> twitterKafkaProducer = KafkaProducerDemoUtils.createKafkaProducer();

        // Loop to send tweets to Kafka
        while (!twitterClient.isDone()) {
            String message = null;
            try {
                message = messageQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedExceptionObj) {
                loggerObject.error(interruptedExceptionObj.getMessage(),interruptedExceptionObj);
                twitterClient.stop();
            }

            if( message != null){
                loggerObject.info(message);
            }
        }
        loggerObject.info("End of the application");
    }

    /**
     * This method would be invoked during Twitter Kafka producer demo
     *
     * @return - Twitter Client object
     * @param msgQueue
     */
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);

        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(TwitterConfig.API_KEY,
                TwitterConfig.API_SECRET_KEY,
                TwitterConfig.ACCESS_TOKEN,
                TwitterConfig.SECRET_ACCESS_TOKEN
        );

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }


}
