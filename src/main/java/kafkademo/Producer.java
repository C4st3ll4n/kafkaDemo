package kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        Properties _properties = new Properties();
        _properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        _properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        _properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> _producer = new KafkaProducer<>(_properties)) {

            ProducerRecord<String, String> _record = new ProducerRecord<>(
                    "hello", "hello darkness !"
            );
            _producer.send(_record);
        }catch (Exception e){
            System.out.println("Deu ruim "+ e);
        }
    }
}
