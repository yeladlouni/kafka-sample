import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomerSerializer implements Serializer {
    private int id;
    private String firstName;
    private String lastName;

    @Override
    public byte[] serialize(String s, Object o) {
        return new byte[0];
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
