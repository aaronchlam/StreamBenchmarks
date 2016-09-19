package benchmarking.kafka.serde;

/**
 * Created by jeka01 on 08/09/16.
 */
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by gwen on 1/12/16.
 */
public class MinMaxDeserializer implements Deserializer<MinMax> {
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public MinMax deserialize(String topic, byte[] data) {
        if (data == null)
            return null;
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long ts = buffer.getLong();
        double maxPrice = buffer.getDouble();
        double minPrice = buffer.getDouble();
        return new MinMax(ts, maxPrice, minPrice);
    }

    public void close() {

    }
}
