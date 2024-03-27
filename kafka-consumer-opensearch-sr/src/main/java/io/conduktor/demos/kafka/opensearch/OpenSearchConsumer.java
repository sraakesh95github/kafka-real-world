package io.conduktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.lucene.index.IndexReader;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

        public static RestHighLevelClient createOpenSearchClient() {
//            String connString = "http://localhost:9200";
        String connString = "https://dcqma8wn43:2mru0dspwg@kafka-learn-6628519627.us-east-1.bonsaisearch.net:443";

            // we build a URI from the connection string
            RestHighLevelClient restHighLevelClient;
            URI connUri = URI.create(connString);
            // extract login information if it exists
            String userInfo = connUri.getUserInfo();

            if (userInfo == null) {
                // REST client without security
                restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

            }
            else {
                // REST client with security
                String[] auth = userInfo.split(":");

                CredentialsProvider cp = new BasicCredentialsProvider();
                cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

                restHighLevelClient = new RestHighLevelClient(
                        RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                                .setHttpClientConfigCallback(
                                        httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                                .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


                }

            return restHighLevelClient;
        }

        private static KafkaConsumer<String, String> createKafkaConsumer() {

            //set initial params
            String groupId = "consumer-opensearch-demo";

            // Create Producer properties
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "https://driving-anemone-14951-us1-kafka.upstash.io:9092");
            properties.put("sasl.mechanism", "SCRAM-SHA-256");
            properties.put("security.protocol", "SASL_SSL");
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ZHJpdmluZy1hbmVtb25lLTE0OTUxJOOUD-QWV9p9W116XVhMgWQkRVx_FvKlBac\" password=\"YWUxMDAyMTAtYWNhOS00OTg5LWEwMmQtOWE4NzdlYmFmZTIx\";");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            // Create consumer config
            properties.put("key.deserializer", StringDeserializer.class.getName());
            properties.put("value.deserializer", StringDeserializer.class.getName());
            properties.put("group.id", groupId);
            properties.put("auto.offset.reset", "latest");
            properties.put("enable.auto.commit", "false"); //Enable this for manual commit

            // create a consumer
            return new KafkaConsumer<>(properties);
        }

        private static String extractId(String json) {
            //using Google GSON library
            return JsonParser.parseString(json)
                    .getAsJsonObject()
                    .get("meta")
                    .getAsJsonObject()
                    .get("id")
                    .getAsString();
        }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        //create an opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //graceful shutdown
        final Thread mainThread = Thread.currentThread();

        //adding teh shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling the consumer wakeup()...");
                consumer.wakeup();

                //join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        //we need to create the index on OpenSearch if it doesn't exist
        try(openSearchClient; consumer) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (indexExists) {
                log.info("The Wikimedia index already exists");
            } else {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The Wikimedia index has been created!");
            }

            //subscribe the consumer
            consumer.subscribe(Collections.singleton("wikimedia.recentchange"));
            log.info("CONSUMER SUBSCRIBED TO TOPIC");

            //main code logic
            //Automatically the following logic does synchronous commit i.e at-least once commit
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                //bulk request let's us perform the getIndex once for a bulk insertion of records
                BulkRequest bulkRequest = new BulkRequest();
                
                for (ConsumerRecord<String, String> record : records) {

                    //strategy 1
                    //define an ID using Kafka Record coordinates

//                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                        //strategy 2
                        //extract the id from the JSON value
                        String id = extractId(record.value());

                        //send the record to opensearch
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);

//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                        bulkRequest.add(indexRequest);

//                        log.info(response.getId());
                    } catch (Exception e) {

                    }

                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkResponse.getItems().length + " record(s)");

                    //adding delays to increase the chances of getting bulk actions
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                //commit offsets after the batch is consumed
                consumer.commitSync();
                log.info("Offsets have been committed");
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // closes the consumer, also peforms commit
            openSearchClient.close(); // openSearch client close
            log.info("The consumer is now gracefully shut down");
        }

        //close the connection


    }
}
