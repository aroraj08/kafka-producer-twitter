package com.kafka.app.twitter;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private static final String CONSUMER_KEY = "consumer_key_to_be_put";
    private static final String CONSUMER_SECRET = "consumer_secret_to_be_put";
    private static final String TOKEN = "generated_token_on_twitter";
    private static final String SECRET = "generated_secret_on_twitter";

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public void run()  {
        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create a Kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // loop to send tweets to Kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                 msg = msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                    e.printStackTrace();
                    client.stop();
                    producer.flush();
                    producer.close();
            }
            if (msg != null) {
                logger.info(msg);
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("twitter_tweets", msg);

                producer.send(record, ((recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Error while producing message to Kafka!");
                    }
                }));
            }
        }
        logger.info("End of application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {

        String serverAddress = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Making Producer Idempotent
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Not required to set explicitly
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));  //32

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }


    private Client createTwitterClient(BlockingQueue<String> msgQueue) {

       /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("USA", "India", "China", "Covid");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET ,TOKEN, SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hoseBirdClient = builder.build();
        return hoseBirdClient;
    }
    public static void main(String[] args) throws InterruptedException {

        new TwitterProducer().run();

    }
}
