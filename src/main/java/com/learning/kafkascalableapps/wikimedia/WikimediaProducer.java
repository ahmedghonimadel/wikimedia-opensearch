package com.learning.kafkascalableapps.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {
    public static void main(String[] args) throws InterruptedException {

        String topic="kafka.learning.orders";
        //Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();

        //List of brokers to connect to
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");

        //Serializer class used to convert Keys to Byte Arrays
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        //Serializer class used to convert Messages to Byte Arrays
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

//        kafkaProps.setProperty("batch.size","100");
        kafkaProps.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        //Set ACKS to all so all replicas needs to acknolwedge
        kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");
        // Set the min.insync.replicas property
//        kafkaProps.put(ProducerConfig.MIN_INSYNC_REPLICAS_CONFIG, "2");
        kafkaProps.setProperty("min.insync.replicas","2");
        //how can avoid duplications
//        kafkaProps.put("enable.idempotence",true);
        kafkaProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        kafkaProps.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        //high throughput producer at the expense of a bit of latency and cpu usage
        //introduce some producer-level compression for more efficiency in sends
        kafkaProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        //increase linger.ms the producer will wait a few milliseconds for the batches to fill up before sending them
        kafkaProps.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        //if you are sending full batches and have memory to spare you can increase bach.size and send larger batches
        kafkaProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

        //Create a Kafka producer from configuration
        KafkaProducer optionsProducer = new KafkaProducer(kafkaProps);

        EventHandler eventHandler=new WikimediaChangeHandler(optionsProducer,topic);
        String url="https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder=new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource=builder.build();

        eventSource.start();

        //produce for 10 minutes
        TimeUnit.MINUTES.sleep(10);
    }
}
