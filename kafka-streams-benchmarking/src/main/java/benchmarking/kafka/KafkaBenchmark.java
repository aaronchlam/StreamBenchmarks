package benchmarking.kafka;

import benchmarking.connector.KafkaToFSConnector;
import benchmarking.connector.SocketKafkaConnector;
import benchmarking.kafka.serde.MinMax;
import benchmarking.kafka.serde.MinMaxSerde;
import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.socket.DataGenerator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.json.JSONObject;

import java.io.FileReader;
import java.net.InetAddress;
import java.util.*;

public class KafkaBenchmark {
        public static void main(String[] args) throws Exception {

            if (args == null || args.length != 1){
                throw new Exception("commandline argument is needed: path to configuration file");
            }
            String confPath = args[0];
            YamlReader reader = new YamlReader(new FileReader(confPath));
            Object object = reader.read();
            Map conf = (Map)object;

            ArrayList<String> bootstrapServersArr = (ArrayList<String>) conf.get("kafka.brokers");
            Integer kafkaPort = new Integer (conf.get("kafka.port").toString());
            String bootstapServers  = "";
            for (String server: bootstrapServersArr) {
                bootstapServers = bootstapServers +server + ":" + kafkaPort + ",";
            }
            bootstapServers = bootstapServers.substring(0, bootstapServers.length()-1);

            ArrayList<String> zookeeperServersArr = (ArrayList<String>) conf.get("zookeeper.servers");
            Integer zookeeperPort = new Integer (conf.get("zookeeper.port").toString());
            String zookeeperServers  = "";
            for (String server: zookeeperServersArr) {
                zookeeperServers = zookeeperServers +server + ":" + zookeeperPort + ",";
            }
            zookeeperServers = zookeeperServers.substring(0, zookeeperServers.length()-1);

            String kafkaTopic = conf.get("kafka.topic").toString();
            String dataGeneratorHost = InetAddress.getLocalHost().getHostName();
            Integer dataGeneratorPort = new Integer(conf.get("datasourcesocket.port").toString());


            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-benchmark");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstapServers);
            props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperServers);
            props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
            props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            int slideWindowLength = new Integer(conf.get("slidingwindow.length").toString());
            int slideWindowSlide = new Integer(conf.get("slidingwindow.slide").toString());

            KStreamBuilder builder = new KStreamBuilder();
            KStream<Long, String> source = builder.stream(Serdes.Long(),Serdes.String(), kafkaTopic);

            KStream<String, MinMax> tsAppended = source.map(new KeyValueMapper<Long, String, KeyValue<String, MinMax>>() {
                @Override
                public KeyValue<String, MinMax > apply(Long aLong, String s) {
                    JSONObject obj = new JSONObject(s);
                    String geo = obj.getJSONObject("t").getString("geo");
                    Double price = obj.getJSONObject("m").getDouble("price");

                    return new KeyValue<String, MinMax>(geo, new MinMax(System.nanoTime(), price, price));
                }
            } );

            KTable<Windowed<String>, MinMax> windowAgg = tsAppended.aggregateByKey(
                    new Initializer<MinMax>() {
                        @Override
                        public MinMax apply() {
                            return new MinMax(0L, Double.MIN_VALUE, Double.MAX_VALUE);
                        }
                    },
                    new AvgAggregator<String, MinMax, MinMax>(),
                    TimeWindows.of("timewindow", slideWindowLength).advanceBy(slideWindowSlide),
                    Serdes.String(), new MinMaxSerde()
            );

            KStream<String, MinMax> dede = windowAgg.toStream().map(new KeyValueMapper<Windowed<String>, MinMax, KeyValue<String, MinMax>>() {
                @Override
                public KeyValue<String, MinMax> apply(Windowed<String> stringWindowed, MinMax minMax) {
                    System.out.println(stringWindowed.window().start()+"-" + stringWindowed.window().end());
                    return new KeyValue<String, MinMax>(stringWindowed.window().hashCode()+"",minMax);
                }
            });

          //  KTable<Windowed<String>, MinMax> windowAggResult  =   windowAgg.mapValues((v)-> {System.out.println(v.getMaxPrice() + " - " + v.getMinPrice() );  return  v;}   )   ;

//            KTable<Windowed<Long>, MinMax> resultStream =  windowAggResult.mapValues( (v) -> {
//                                                                                                v.setTs( System.nanoTime() - v.getTs() );
//                                                                                                System.out.println(v.getSum() + " - " + v.getCount() );
//                                                                                                return v; } );



            String kafkaoutputTopic = conf.get("kafkastream.output.topic").toString();
            String outputFileLocation = conf.get("kafka.output").toString();
         //   resultStream.to(Serdes.String(), Serdes.Long(),kafkaoutputTopic);
           // resultStream.print(Serdes.Long(), new MinMaxSerde());
            // need to override value serde to Long type
           // counts.to(Serdes.String(), Serdes.Long(), "streams-wordcount-output");

            Long benchmarkingCount = new Long(conf.get("benchmarking.count").toString());
            Long warmupCount = new Long(conf.get("warmup.count").toString());
            Long sleepTime = new Long(conf.get("datagenerator.sleep").toString());

            DataGenerator.generate(dataGeneratorPort, benchmarkingCount, warmupCount, sleepTime);

            Thread.sleep(1000L);

            SocketKafkaConnector.read(kafkaTopic, bootstapServers, dataGeneratorHost, dataGeneratorPort);

            KafkaToFSConnector.writeToFile(kafkaoutputTopic,bootstapServers,outputFileLocation);
            Thread.sleep(1000L);

            KafkaStreams streams = new KafkaStreams(builder, props);
            streams.start();
        }


    public static class AvgAggregator<K, V, T>  implements Aggregator<String, MinMax, MinMax> {
        @Override
        public MinMax apply(String key, MinMax val, MinMax aggr) {
            Double maxPrice = Math.max(val.getMaxPrice(), aggr.getMaxPrice());
            Double minPrice = Math.min(val.getMinPrice(), aggr.getMinPrice());
            Long ts = Math.max(val.getTs(), aggr.getTs());
            MinMax newAggr = new MinMax(ts, maxPrice, minPrice);
            return newAggr;
        }
    }


}
