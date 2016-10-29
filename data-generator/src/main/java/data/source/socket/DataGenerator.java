package data.source.socket;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by jeka01 on 02/09/16.
 */
public class DataGenerator extends Thread {
    private int benchmarkCount;
    private long sleepTime;
    private int blobSize;
    private boolean isRandomGeo;
    private static Double partition;
    private boolean putTs;
    private AdsEvent adsEvent;
    private final KafkaProducer<Integer, String> producer;
    private String topic;

    private DataGenerator(HashMap conf) throws IOException {
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString());
        this.sleepTime = new Long(conf.get("datagenerator.sleep").toString());
        this.blobSize = new Integer(conf.get("datagenerator.blobsize").toString());
        this.isRandomGeo = new Boolean(conf.get("datagenerator.israndomgeo").toString());
        this.putTs = new Boolean(conf.get("datagenerator.ts").toString());
        adsEvent = new AdsEvent(isRandomGeo, putTs, partition);
        this.topic = conf.get("kafka.topic").toString();
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "DemoProducer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);

    }

    public void run() {
        try {
            sendTuples(benchmarkCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void sendTuples(int tupleCount) throws Exception {
        long currTime = System.currentTimeMillis();
        if (sleepTime != 0) {
            for (int i = 0; i < tupleCount; ) {
                Thread.sleep(sleepTime);
                for (int b = 0; b < blobSize && i < tupleCount; b++, i++) {
                    producer.send(new ProducerRecord<>(topic,
                            i,
                            adsEvent.generateJson()));
                    if (i % 100000 == 0){
                        System.out.println("Here we go");
                    }
                }
            }
        } else {
            for (int i = 0; i < tupleCount; ) {
                for (int b = 0; b < blobSize && i < tupleCount; b++, i++) {

                    producer.send(new ProducerRecord<>(topic,
                                    i,
                            adsEvent.generateJson()));

                    if (i % 100000 == 0){
                        System.out.println("Here we go");
                    }
                }
            }
        }
        long runtime = (currTime - System.currentTimeMillis()) / 1000;
        System.out.println("Benchmark producer data rate is " + tupleCount / runtime + " ps");
    }

    public static void main(String[] args) throws Exception {
        String confFilePath = args[0];
        partition = new Double(args[1]);
        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        HashMap conf = (HashMap) object;

        try {
            Thread generator = new DataGenerator(conf );
            generator.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void writeHashMapToCsv(HashMap<Long, Integer> hm, String path)  {
        try{
            File file = new File(path.split("\\.")[0]+ "-" + InetAddress.getLocalHost().getHostName() + ".csv");

            if (file.exists()) {
                file.delete(); //you might want to check if delete was successfull
            }
            file.createNewFile();
            FileOutputStream fileOutput = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fileOutput));
            Iterator it = hm.entrySet().iterator();
            while (it.hasNext()) {
                HashMap.Entry pair = (HashMap.Entry)it.next();
                bw.write(pair.getKey()+ "," + pair.getValue() + "\n");
            }
            bw.flush();
        } catch (Exception e){
            e.printStackTrace();
        }

    }


}

