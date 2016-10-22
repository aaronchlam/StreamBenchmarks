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
public class DataGeneratorHelper extends Thread {
    private int benchmarkCount;
    private long sleepTime;
    private int blobSize;
    private boolean isRandomGeo;
    private static Double partition;
    private boolean putTs;
    private BlockingQueue<JSONObject> buffer;
    private AdsEvent adsEvent;
    private Control control;
    private DataGeneratorHelper(HashMap conf, BlockingQueue<JSONObject> buffer, Control control) throws IOException {
        this.buffer = buffer;
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString()) / new Integer(conf.get("datagenerator.count").toString());
        this.sleepTime = new Long(conf.get("datagenerator.sleep").toString());
        this.blobSize = new Integer(conf.get("datagenerator.blobsize").toString());
        this.isRandomGeo = new Boolean(conf.get("datagenerator.israndomgeo").toString());
        this.putTs = new Boolean(conf.get("datagenerator.ts").toString());
        adsEvent = new AdsEvent(isRandomGeo, putTs, partition);
        this.control = control;
    }

    public void run() {
        try {
            sendTuples(benchmarkCount);
            control.stop = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void sendTuples(int tupleCount) {
        long currTime = System.currentTimeMillis();
        for (int i = 0; i < tupleCount; ) {
            try {
                if (sleepTime != 0)
                    Thread.sleep(sleepTime);
                for (int b = 0; b < blobSize && i < tupleCount; b++, i++) {
                    JSONObject tuple = adsEvent.generateJson();
                    buffer.put(tuple);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        long runtime = (currTime - System.currentTimeMillis()) / 1000;
        System.out.println("Benchmark producer data rate is " + tupleCount / runtime + " ps");

    }

    public static void execute(HashMap conf, Integer port, Double p)  {
        partition = p;

        try {

            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.setSoTimeout(900000);
            System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
            Socket server = serverSocket.accept();
            System.out.println("Just connected to " + server.getRemoteSocketAddress());
            PrintWriter out = new PrintWriter(server.getOutputStream(), true);
            Integer generatorCount = new Integer(conf.get("datagenerator.count").toString());
            int bufferSize = new Integer(conf.get("benchmarking.count").toString());
            BlockingQueue<JSONObject> buffer = new ArrayBlockingQueue<JSONObject>(bufferSize);    // new LinkedBlockingQueue<>();
            final Control control = new Control();



            for (int i = 0; i < generatorCount; i++) {
                Thread generator = new DataGeneratorHelper(conf, buffer, control);
                generator.start();
            }
            Thread bufferReader = new BufferReader(buffer, conf, out, serverSocket);
            bufferReader.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class BufferReader extends Thread {
    private BlockingQueue<JSONObject> buffer;
    private Logger logger = Logger.getLogger("MyLog");
    private PrintWriter out;
    private ServerSocket serverSocket;
    private int benchmarkCount;
    public BufferReader(BlockingQueue<JSONObject> buffer, HashMap conf, PrintWriter out, ServerSocket serverSocket) {
        this.buffer = buffer;
        this.out = out;
        this.serverSocket = serverSocket;
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString());
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
            long timeStart = System.currentTimeMillis();
            for (int i = 0; i < benchmarkCount; i++) {
                JSONObject tuple = buffer.take();
                out.println(tuple.toString());
            }
            long timeEnd = System.currentTimeMillis();
            long runtime = (timeEnd - timeStart) / 1000;
            long throughput = benchmarkCount / runtime;
            logger.info("---BENCHMARK ENDED--- on " + runtime + " seconds with " + throughput + " throughput "
                    + " node : " + InetAddress.getLocalHost().getHostName());
            logger.info("Waiting for client on port " + serverSocket.getLocalPort() + "...");
            Socket server = serverSocket.accept();


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}

class Control {
    public volatile boolean stop = false;
}

