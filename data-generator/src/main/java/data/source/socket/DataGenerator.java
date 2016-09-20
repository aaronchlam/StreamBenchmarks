package data.source.socket;

import com.esotericsoftware.yamlbeans.YamlException;
import data.source.model.AdsEvent;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * Created by jeka01 on 02/09/16.
 */
public class DataGenerator extends Thread {
    private ServerSocket serverSocket;
    private long benchmarkCount;
    private long warmupCount;
    private long sleepTime;

    private DataGenerator(int port, long benchmarkCount, long warmupCount, long sleepTime) throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(100000);
        this.benchmarkCount = benchmarkCount;
        this.warmupCount = warmupCount;
        this.sleepTime = sleepTime;
    }

    public void run() {
        AdsEvent dg = new AdsEvent();
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
                        JSONObject obj = dg.generateJson(true);
                        out.println(obj.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                for (long i = 0; i < benchmarkCount; i++) {
                    try {
                        if (sleepTime != 0)
                            Thread.sleep(sleepTime);
                        JSONObject obj = dg.generateJson(false);
                        out.println(obj.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                System.out.println("---CURRENT BENCHMARK ENDED----");
            } catch (SocketTimeoutException s) {
                System.out.println("Socket timed out!");
                break;
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    public static void generate(int port, long benchmarkingCount, long warmupCount, long sleepTime) throws YamlException, FileNotFoundException {
        try {
            Thread t = new DataGenerator(port, benchmarkingCount, warmupCount, sleepTime);
            t.start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
