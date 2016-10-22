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
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Created by jeka01 on 02/09/16.
 */
public class DataGenerator extends Thread {
    private int benchmarkCount;
    private int warmupCount;
    private long sleepTime;
    private int blobSize;
    private boolean isRandomGeo;
    private static Double partition;
    private boolean putTs;
    private BlockingQueue<JSONObject> buffer;
    private AdsEvent adsEvent;

    private DataGenerator(HashMap conf, BlockingQueue<JSONObject> buffer) throws IOException {
        this.buffer = buffer;
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString())/  new Integer(conf.get("datagenerator.count").toString()) ;
        this.warmupCount = new Integer(conf.get("warmup.count").toString()) / new Integer(conf.get("datagenerator.count").toString());
        this.sleepTime = new Long(conf.get("datagenerator.sleep").toString());
        this.blobSize = new Integer(conf.get("datagenerator.blobsize").toString());
        this.isRandomGeo = new Boolean(conf.get("datagenerator.israndomgeo").toString());
        this.putTs = new Boolean(conf.get("datagenerator.ts").toString());
        adsEvent = new AdsEvent(isRandomGeo, putTs, partition);
    }

    public void run() {
        try {
            sendTuples(warmupCount, true);
            while (!buffer.isEmpty()) {
                currentThread().sleep(8000);
                System.out.println("sleeeeppppp");
            }
            sendTuples(benchmarkCount, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void sendTuples(int tupleCount, boolean isWarmUp) {
        long currTime = System.currentTimeMillis();
        for (int i = 0; i < tupleCount; ) {
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
        if (tupleCount != 0){
            if (isWarmUp )
                System.out.println("Warmup data rate is " + tupleCount / ((currTime - System.currentTimeMillis()) / 1000) + " ps");
            else
                System.out.println("Benchmark data rate is " + tupleCount / ((currTime - System.currentTimeMillis()) / 1000) + " ps");
        }
    }

    public static void main(String[] args) throws Exception {
        String confFilePath = args[0];
        partition = new Double(args[1]);
        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        HashMap conf = (HashMap) object;

        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(900000);
        System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
        Socket server = serverSocket.accept();
        System.out.println("Just connected to " + server.getRemoteSocketAddress());
        PrintWriter out = new PrintWriter(server.getOutputStream(), true);
        Integer generatorCount = new Integer(conf.get("datagenerator.count").toString());
        int bufferSize =   new Integer(conf.get("benchmarking.count").toString()) + new Integer(conf.get("warmup.count").toString());
	 BlockingQueue<JSONObject> buffer = new ArrayBlockingQueue<JSONObject>(bufferSize);    // new LinkedBlockingQueue<>();
        try {
            for (int i = 0; i < generatorCount ; i++){
                Thread generator = new DataGenerator(conf, buffer);
                Thread bufferReader = new BufferReader(buffer, conf, out, serverSocket);
                generator.start();
                bufferReader.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class BufferReader extends Thread {
    private BlockingQueue<JSONObject> buffer;
    private Logger logger = Logger.getLogger("MyLog");
    private int bufferElements;
    private long benchmarkCount;
    private PrintWriter out;
    private ServerSocket serverSocket;

    public BufferReader(BlockingQueue<JSONObject> buffer, HashMap conf, PrintWriter out, ServerSocket serverSocket) {
        this.buffer = buffer;
        this.bufferElements = (new Integer(conf.get("benchmarking.count").toString()) + new Integer(conf.get("warmup.count").toString())) / new Integer(conf.get("datagenerator.count").toString()) ;
        this.benchmarkCount = new Long(conf.get("benchmarking.count").toString());
        this.out = out;
        this.serverSocket = serverSocket;
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
                if (warmupContinues) {
                    if (!tuple.getBoolean("isDummy")) {
                        warmupContinues = false;
                        timeStart = System.currentTimeMillis();
                        logger.info("---WARMUP ENDED---  ");
                    }
                }
                if (i % 1000000 == 0)
                    logger.info((bufferElements - i) + " tuples left in buffer  ");
                out.println(tuple.toString());
            }
            long timeEnd = System.currentTimeMillis();
            Long throughput = benchmarkCount / ((timeEnd - timeStart) / 1000);
            logger.info("---BENCHMARK ENDED--- on " + (timeEnd - timeStart) / 1000 + " seconds with " + throughput + " throughput"
                    + InetAddress.getLocalHost().getHostName());
            logger.info("Waiting for client on port " + serverSocket.getLocalPort() + "...");
            Socket server = serverSocket.accept();


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
