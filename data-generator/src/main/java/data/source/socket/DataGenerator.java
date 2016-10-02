package data.source.socket;

import data.source.model.AdsEvent;
import org.json.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;

/**
 * Created by jeka01 on 02/09/16.
 */
public class DataGenerator extends Thread {
    private ServerSocket serverSocket;
    private long benchmarkCount;
    private long warmupCount;
    private long sleepTime;
    private long blobSize;
    private boolean isRandomGeo;

    private boolean putTs;

    private DataGenerator( HashMap conf) throws IOException {
        this.benchmarkCount = new Long(conf.get("benchmarking.count").toString());
        this.warmupCount = new Long(conf.get("warmup.count").toString());
        this.sleepTime = new Long(conf.get("datagenerator.sleep").toString());
        this.blobSize = new Long(conf.get("datagenerator.blobsize").toString());
        this.isRandomGeo = new Boolean(conf.get("datagenerator.israndomgeo").toString());
        this.putTs = new Boolean(conf.get("datagenerator.ts").toString());
        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(20000);
    }

    public void run() {
        AdsEvent dg = new AdsEvent(isRandomGeo,putTs);
        while (true) {
            try {
                System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
                Socket server = serverSocket.accept();
                System.out.println("Just connected to " + server.getRemoteSocketAddress());
                PrintWriter out = new PrintWriter(server.getOutputStream(), true);

                for (long i = 0; i < warmupCount; i++) {
                    try {
                        if (sleepTime != 0)
                            Thread.sleep(sleepTime);
                        for (int b = 0; b < blobSize && i < warmupCount; b++, i++) {
                            JSONObject obj = dg.generateJson(true);
                            out.println(obj.toString());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                System.out.println("---WARMUP BENCHMARK ENDED----");
                long currTime = System.currentTimeMillis();
                long currIndex = 0L;
                long thoughput = 0L;
                long throughputCount = 0L;
                for (long i = 0; i < benchmarkCount; i++) {
                    if (currTime + 1000L < System.currentTimeMillis()) {
                        currTime = System.currentTimeMillis();
                        thoughput = thoughput + (i - currIndex);
                     //   System.out.println("Throughput is:" + (i - currIndex));
                        currIndex = i;
                        throughputCount++;
                    }
                    try {
                        if (sleepTime != 0)
                            Thread.sleep(sleepTime);
                        for (int b = 0; b < blobSize && i < benchmarkCount; b++, i++) {
                            JSONObject obj = dg.generateJson(false);
                            out.println(obj.toString());
                           // System.out.println("DG-" + obj.getJSONObject("t").getString("geo"));
                            System.out.println(obj.getJSONObject("t").getString("geo")+ " - " + obj.getJSONObject("m").getString("price"));

                        }
                        //                      System.out.println(obj.toString() +" gone");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("\n \n ---CURRENT BENCHMARK ENDED---- \n \n Throughtput is " + (thoughput / throughputCount));
            } catch (SocketTimeoutException s) {
                System.out.println("Socket timed out!");
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }


    public static void generate(HashMap conf) throws Exception {

        try {
            Thread t = new DataGenerator(conf);
            t.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}
