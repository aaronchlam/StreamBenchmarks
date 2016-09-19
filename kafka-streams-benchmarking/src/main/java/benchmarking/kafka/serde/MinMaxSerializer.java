package benchmarking.kafka.serde;

/**
 * Created by jeka01 on 08/09/16.
 */
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class MinMaxSerializer implements Serializer<MinMax> {

    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    public byte[] serialize(String topic, MinMax data) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 8);
        buffer.putLong(data.ts);
        buffer.putDouble(data.maxPrice);
        buffer.putDouble(data.minPrice);

        return buffer.array();
    }

    public void close() {
    }
}
