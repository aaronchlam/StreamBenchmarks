package benchmarking.kafka.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by jeka01 on 08/09/16.
 */
public class MinMaxSerde implements Serde<MinMax>{
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<MinMax> serializer() {
        return new MinMaxSerializer();
    }

    @Override
    public Deserializer<MinMax> deserializer() {
        return new MinMaxDeserializer();
    }
}
