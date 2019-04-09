package com.WorlNewsAPI;


import org.apache.kafka.clients.producer.ProducerConfig;

import java.lang.reflect.Array;
import java.util.Properties;

public class NewsProducer {

    public static final String topic = "HelloNews";

    static void start() throws InterruptedException{

        /*
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9022");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("client.id", "camus");

        ProducerConfig producerConfig = new ProducerConfig(properties);

        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(topic, producerConfig);*/

        //client key
        String key = NewsConstant.apiKey;

        //Declare variables
        String news_url = "https://newsapi.org/v2/top-headlines?country=us&apiKey=API_KEY";
        String queryTopic = "bitcoin";
        String source = "bbc-news";
        String category = "business";
        String country = "us";

        // top headlines
        String top_headlines = news_url;



    }
    public static void main(String[] args){
        System.out.println("News thread started...");

//        NewsProducer.start();
    }

}
