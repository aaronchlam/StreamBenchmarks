package benchmarking.connector;

/**
 * Created by jeka01 on 06/09/16.
 */

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class KafkaToFSConnector extends ShutdownableThread {
    private final KafkaConsumer<String, Long> consumer;
    private final String topic;
    private BufferedWriter bw;

    private volatile boolean running = true;

    public void terminate() {
        running = false;
    }


    public KafkaToFSConnector(String topic, String bootstapServers, String outputFilePath) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;

        try {
            File file = new File(outputFilePath);

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.getParentFile().mkdirs();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            bw = new BufferedWriter(fw);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void doWork() {
        while (running) {
            consumer.subscribe(Collections.singletonList(this.topic));
            ConsumerRecords<String, Long> records = consumer.poll(1000);
            for (ConsumerRecord<String, Long> record : records) {
                //System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                try {
                    bw.write(record.key() + " --- " + record.value() + "\n");
                    bw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void writeToFile(String topic, String bootstapServers, String outputFilePath){
        KafkaToFSConnector toFile = new KafkaToFSConnector(topic,bootstapServers,outputFilePath);
        toFile.start();
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}
