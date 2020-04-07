import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.util.Properties;

public class ProducerExample {

    private KafkaProducer<String, String> producer;

    public ProducerExample() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer(kafkaProps);
    }

    // Fire and forget
    private void send() {
        ProducerRecord<String, String> record =
                new ProducerRecord("CustomerCountry", "Precision Products", "France");
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Sync
    private void sendSync() {
        ProducerRecord<String, String> record =
                new ProducerRecord("CustomerCountry", "Precision Products", "France");

        try {
            RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
            
            System.out.println(metadata.topic() + ";" + metadata.offset());


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class DemoProducerCallback implements Callback {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            System.out.println(recordMetadata.topic() + ";" + recordMetadata.offset());
            if (e != null) {
                e.printStackTrace();
            }
        }
    }

    // Async
    private void sendAsync() {
        ProducerRecord<String, String> record =
                new ProducerRecord("CustomerCountry", "Biomedical Materials", "USA");
        producer.send(record, new DemoProducerCallback());
    }

    public static void main(String[] args) {
        ProducerExample example = new ProducerExample();
        example.send();
        example.sendSync();
        example.sendAsync();
    }
}

