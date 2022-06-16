package kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        Properties _properties = new Properties();
        _properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        _properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        _properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        _properties.put(ConsumerConfig.GROUP_ID_CONFIG, "abc");
        _properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> _consumer = new KafkaConsumer<>(_properties)) {
            _consumer.subscribe(List.of("hello"));
            while (true) {
                ConsumerRecords<String, String> _records = _consumer.poll(Duration.ofSeconds(2));

                for (ConsumerRecord<String, String> rec :
                        _records) {
                    System.out.println("record = " + rec.value());
                }
            }
        }
    }
}
