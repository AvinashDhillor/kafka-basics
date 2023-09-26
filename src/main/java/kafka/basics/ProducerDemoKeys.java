package kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(Producer.class.getSimpleName());

    public static void main(String[] args) {
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // ** in this case the topic will be send in different partitions.
        // ** but if we use loop then everything will go into same partition due to
        // sticky session.(performance improvement)
        // Create a producer record
        for (int x = 0; x < 3; x++) {

            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello_world" + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key,
                        value);

                // send data ->test
                producer.send(producerRecord, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // execute every time a record success sent or an exception is thrown
                        if (exception == null) {
                            log.info("Received new metadata \n" +
                                    "Key: " + key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Exception had occurred", exception);
                        }
                    }

                });
            }
        }

        // flush and close the producer -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
