package data.source.socket;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEvent;
import org.json.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Created by jeka01 on 02/09/16.
 */
public class DataGenerator extends Thread {
    private long benchmarkCount;
    private long warmupCount;
    private long sleepTime;
    private long blobSize;
    private boolean isRandomGeo;
    private static Double partition;
    private boolean putTs;
    private BlockingQueue<JSONObject> buffer;
    private AdsEvent adsEvent;

    private DataGenerator(HashMap conf, BlockingQueue<JSONObject> buffer) throws IOException {
        this.buffer = buffer;
        this.benchmarkCount = new Long(conf.get("benchmarking.count").toString());
        this.warmupCount = new Long(conf.get("warmup.count").toString());
        this.sleepTime = new Long(conf.get("datagenerator.sleep").toString());
        this.blobSize = new Long(conf.get("datagenerator.blobsize").toString());
        this.isRandomGeo = new Boolean(conf.get("datagenerator.israndomgeo").toString());
        this.putTs = new Boolean(conf.get("datagenerator.ts").toString());
        adsEvent = new AdsEvent(isRandomGeo, putTs, partition);
    }

    public void run() {
        while (true) {
            sendTuples(warmupCount, true);
            sendTuples(benchmarkCount, false);
            long currTime = System.currentTimeMillis();
            long duration = (System.currentTimeMillis() - currTime) / 1000;
        }
    }


    private void sendTuples(long tupleCount, boolean isWarmUp) {
        for (long i = 0; i < tupleCount; i++) {
            try {
                if (sleepTime != 0)
                    Thread.sleep(sleepTime);
                for (int b = 0; b < blobSize && i < tupleCount; b++, i++) {
                    JSONObject tuple = adsEvent.generateJson(isWarmUp);
                    buffer.put(tuple);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String confFilePath = args[0];
        partition = new Double(args[1]);
        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        HashMap conf = (HashMap) object;

        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        ServerSocket  serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(900000);
        System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
        Socket server = serverSocket.accept();
        System.out.println("Just connected to " + server.getRemoteSocketAddress());
        PrintWriter out = new PrintWriter(server.getOutputStream(), true);
        BlockingQueue<JSONObject> buffer = new LinkedBlockingQueue<>();
        try {
            Thread generator = new DataGenerator(conf, buffer);
            Thread bufferReader = new BufferReader(buffer,conf, out);
            generator.start();
            bufferReader.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class BufferReader extends Thread {
    private BlockingQueue<JSONObject> buffer;
    private Logger logger = Logger.getLogger("MyLog");
    private long bufferElements;
    private PrintWriter out;
    public BufferReader(BlockingQueue<JSONObject> buffer, HashMap conf, PrintWriter out) {
        this.buffer = buffer;
        this.bufferElements = new Long(conf.get("benchmarking.count").toString()) + new Long(conf.get("warmup.count").toString());
        this.out = out;
        try {
            String logFile = conf.get("datasource.logs").toString();
            FileHandler fh = new FileHandler(logFile);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
            logger.addHandler(fh);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            long timeStart = 0;

            boolean warmupContinues = true;
            for (long i = 0; i < bufferElements; i++) {
                JSONObject tuple = buffer.take();
                if (warmupContinues){
                    if (! tuple.getBoolean("isDummy")){
                        warmupContinues = false;
                        timeStart = System.currentTimeMillis();
                    }
                }
                out.println(tuple.toString());
            }
            long timeEnd = System.currentTimeMillis();
            logger.info("BENCHMARK ENDED on " + (timeStart - timeEnd) + " milliseconds "
                    + InetAddress.getLocalHost().getHostName());

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
