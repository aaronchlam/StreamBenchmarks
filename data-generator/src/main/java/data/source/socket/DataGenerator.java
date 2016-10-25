package data.source.socket;

import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.model.AdsEvent;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

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
    private BlockingQueue<String> buffer;
    private AdsEvent adsEvent;
    private HashMap<Long, Integer> bufferSizeAtTime = new HashMap<>();
    private  String statisticsBufferSizeFile;
    private int statisticsPeriod;
    private int dataRateIndex = 0;
    private int dataRateIndexPrevVal = 0;

    private HashMap<Long,Integer> dataGenRate = new HashMap<>();
    private  String statisticsDataRateFile;


    private DataGenerator(HashMap conf, BlockingQueue<String> buffer) throws IOException {
        this.buffer = buffer;
        this.benchmarkCount = new Integer(conf.get("benchmarking.count").toString());
        this.sleepTime = new Long(conf.get("datagenerator.sleep").toString());
        this.blobSize = new Integer(conf.get("datagenerator.blobsize").toString());
        this.isRandomGeo = new Boolean(conf.get("datagenerator.israndomgeo").toString());
        this.putTs = new Boolean(conf.get("datagenerator.ts").toString());
        adsEvent = new AdsEvent(isRandomGeo, putTs, partition);
        statisticsBufferSizeFile = conf.get("datagenerator.statistics.buffer").toString();
        statisticsPeriod = new Integer(conf.get("datagenerator.statistics.period").toString());
        statisticsDataRateFile = conf.get("datagenerator.statistics.datarate").toString();
    }

    public void run() {
        try {
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    long interval = (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) / statisticsPeriod ) * statisticsPeriod;
                    int bufferSize = buffer.size();
                    bufferSizeAtTime.put(interval, bufferSize);
                    dataGenRate.put(interval, dataRateIndex - dataRateIndexPrevVal);
                    dataRateIndexPrevVal = dataRateIndex;
                }
            }, 0, statisticsPeriod - 2, TimeUnit.SECONDS);

            sendTuples(benchmarkCount);
            BufferReader.writeHashMapToCsv(bufferSizeAtTime, statisticsBufferSizeFile);
            BufferReader.writeHashMapToCsv(dataGenRate,statisticsDataRateFile);

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
                    buffer.put(adsEvent.generateJson());
                    dataRateIndex = i;
                }
            }
        } else {
            for (int i = 0; i < tupleCount; ) {
                for (int b = 0; b < blobSize && i < tupleCount; b++, i++) {
                    buffer.put(adsEvent.generateJson());
                    dataRateIndex = i;
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

        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(900000);
        System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
        Socket server = serverSocket.accept();
        System.out.println("Just connected to " + server.getRemoteSocketAddress());
        PrintWriter out = new PrintWriter(server.getOutputStream(), true);
        int bufferSize = new Integer(conf.get("benchmarking.count").toString());
        BlockingQueue<String> buffer = new ArrayBlockingQueue<String>(bufferSize);    // new LinkedBlockingQueue<>();
        try {
            Thread generator = new DataGenerator(conf, buffer);
            generator.start();
            Thread bufferReader = new BufferReader(buffer, conf, out, serverSocket);
            bufferReader.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class BufferReader extends Thread {
    private BlockingQueue<String> buffer;
    private Logger logger = Logger.getLogger("MyLog");
    private PrintWriter out;
    private ServerSocket serverSocket;
    private int benchmarkCount;
    private int thourhputIndex = 0;
    private int thourhputPrevVal = 0;
    private  String statisticsThroughputFile;
    private int statisticsPeriod;
    private HashMap<Long,Integer> thoughputCount = new HashMap<>();

    public BufferReader(BlockingQueue<String> buffer, HashMap conf, PrintWriter out, ServerSocket serverSocket) {
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
        statisticsThroughputFile = conf.get("datagenerator.statistics.thoughput").toString();
        statisticsPeriod = new Integer(conf.get("datagenerator.statistics.period").toString());
    }

    public void run() {
        try {
            long timeStart = System.currentTimeMillis();

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    long period = (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) / statisticsPeriod) * statisticsPeriod;
                    thoughputCount.put(period, thourhputIndex - thourhputPrevVal);
                    thourhputPrevVal = thourhputIndex;
                    logger.info(thourhputIndex + " tuples sent from buffer");
                }
            }, 0, statisticsPeriod-2, TimeUnit.SECONDS);


            for (int i = 0; i < benchmarkCount; i++) {
                String tuple = buffer.take();
                out.println(tuple);
                thourhputIndex = i;
            }
            long timeEnd = System.currentTimeMillis();
            long runtime = (timeEnd - timeStart) / 1000;
            long throughput = benchmarkCount / runtime;
            BufferReader.writeHashMapToCsv(thoughputCount,statisticsThroughputFile);

            logger.info("---BENCHMARK ENDED--- on " + runtime + " seconds with " + throughput + " throughput "
                    + " node : " + InetAddress.getLocalHost().getHostName());
            logger.info("Waiting for client on port " + serverSocket.getLocalPort() + "...");
            Socket server = serverSocket.accept();


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


