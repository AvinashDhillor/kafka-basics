package kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // ** in this case the topic will be send in different partitions.
        // ** but if we use loop then everything will go into same partition due to
        // sticky session.(performance improvement)
        // Create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("demo_java", "hello_world");

        // send data ->
        producer.send(producerRecord);

        // flush and close the producer -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
        log.info("Finished");
    }
}
