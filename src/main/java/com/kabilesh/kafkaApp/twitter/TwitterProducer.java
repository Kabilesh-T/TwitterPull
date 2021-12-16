package com.kabilesh.kafkaApp.twitter;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    //Twitter Auth keys
    String consumerKey = "wgazJDKTcfXit3Ij1iip2TYmq";
    String consumerSecret = "ackG8505WSeCeWFOTtZdqX5qr6NcMsqfhn8H59ALZ9pibmxGDR";
    String token = "1370982319792680962-UmkQfKISDfou0Ab9EflnZxhlArHpPR";
    String tokenSecret = "30dlhlQQCYSjT8RMLbFX3IIzrIeh2yexxDVWZPTwD652z";
    String tweetTopic = "tesla";

    //Kafka Producer Properties
    String bootstrapServer = "127.0.0.1:9092";
    String topic = "twitter_tweet";

    List<String> terms = Lists.newArrayList(tweetTopic);

    public TwitterProducer(){}

    public void TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {

        //Create Twitter client
        /* Set up blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        //Create Kafka producer
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        //Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught the shutdown hook");
            logger.info("Shutting down twitter client...");
            client.stop();
            logger.info("Shutting down Kafka producer...");
            kafkaProducer.close();
            logger.info("Out and over !!!");
        }));

        //Loop in to broadcast tweets
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if( msg != null) {
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null ) {
                            logger.error("An error occured", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /* Declare the host to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return  hosebirdClient;
    }


    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        return  kafkaProducer;
    }

}
