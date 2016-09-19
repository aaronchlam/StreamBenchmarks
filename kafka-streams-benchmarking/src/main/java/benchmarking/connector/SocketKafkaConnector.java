package benchmarking.connector;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.io.*;
import java.net.Socket;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by jeka01 on 05/09/16.
 */


public class SocketKafkaConnector extends Thread {
    private final KafkaProducer<Long, String> producer;
    private final String topic;
    private BufferedReader in;
    private volatile boolean running = true;

    public void terminate() {
        running = false;
    }


    private SocketKafkaConnector(String topic, String bootstapServers, String dataSourceServerHost, Integer dataSourceServerPort) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstapServers);
        props.put(StreamsConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        this.topic = topic;

        try {
            Socket clientSocket = new Socket(dataSourceServerHost, dataSourceServerPort);
            InputStream inFromServer = clientSocket.getInputStream();
            DataInputStream reader = new DataInputStream(inFromServer);
            in = new BufferedReader(new InputStreamReader(reader, "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        long messageNo = 1;
        while (running) {
            try {
                String jsonStr = in.readLine();
                producer.send(new ProducerRecord<>(topic,
                        new Long(messageNo),
                        jsonStr)).get();

            } catch (Exception e) {
                e.printStackTrace();
            }
            messageNo++;

        }
    }

    public static void read(String topic, String bootstapServers, String dataGeneratorrHost, Integer dataGeneratorPort){
        SocketKafkaConnector connector = new SocketKafkaConnector(topic, bootstapServers, dataGeneratorrHost, dataGeneratorPort);
        connector.start();
    }

}

