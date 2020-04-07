import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class ConsumerExample {

    private KafkaConsumer<String, String> consumer;
    private Logger logger;

    public ConsumerExample() {
        logger = LoggerFactory.getLogger(ConsumerExample.class);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer(props);
    }

    public void consume() {
        consumer.subscribe(Collections.singletonList("CustomerCountry"));

        try {
            while (true) {
                Duration duration = Duration.ofMillis(100);
                ConsumerRecords<String, String> records = consumer.poll(duration);
                HashMap<String, Integer> custCountryMap = new HashMap();

                for (ConsumerRecord<String, String> record: records) {
                    logger.debug("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    int updatedCount = 1;
                    if (custCountryMap.containsValue(record.value())) {
                        updatedCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updatedCount);

                    JSONObject json = new JSONObject(custCountryMap);
                    System.out.println(json.toString(4));


                }
            }
        } finally {
            consumer.close();
        }
    }

    public static void main(String[] args) {
        ConsumerExample example = new ConsumerExample();
        example.consume();
    }
}
