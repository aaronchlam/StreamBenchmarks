package data.source.socket;

import com.esotericsoftware.yamlbeans.YamlReader;
import org.json.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jeka01 on 02/09/16.
 */
public class DataGenerator extends Thread {
    private List<BufferedReader> bufReaders;
    private PrintWriter out;

    private DataGenerator(HashMap conf, PrintWriter out) throws IOException {
        System.out.println("dede");

        List<String> urls = (List<String>) conf.get("datasourcesocket.helpers");
        bufReaders = new ArrayList<>();
        for (String address : urls) {
            String host = address.split(":")[0];
            Integer port = new Integer(address.split(":")[1]);
            Socket clientSocket = new Socket(host, port);
            System.out.println("clientSocket " + clientSocket);

            InputStream inFromServer = clientSocket.getInputStream();
            DataInputStream reader = new DataInputStream(inFromServer);
            BufferedReader in = new BufferedReader(new InputStreamReader(reader, "UTF-8"));
            bufReaders.add(in);
        }
        this.out = out;

    }

    public void run() {
        try {
            int count = 0;
            while (true) {
                for (BufferedReader bf : bufReaders) {
                    out.println(bf.readLine());
                    System.out.println("dede");
                    count++;
                    if (count % 100000 == 0)
                        System.out.println(count + " tuples sent from buffer");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        String confFilePath = args[0];
        YamlReader reader = new YamlReader(new FileReader(confFilePath));
        Object object = reader.read();
        HashMap conf = (HashMap) object;

        startFeedServers(conf);
        startMainServer(conf);

    }

    private static void startFeedServers(HashMap conf) throws Exception{
        List<String> urls = (List<String>) conf.get("datasourcesocket.helpers");
        for (String address : urls) {
            Integer port = new Integer(address.split(":")[1]);
            Double partition = new Double(address.split(":")[2]);
            Thread t = new StartFeedSockets(port,conf,partition);
            t.start();
        }
    }

    private static void startMainServer(HashMap conf) throws Exception{
        Integer port = new Integer(conf.get("datasourcesocket.port").toString());
        ServerSocket serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(900000);
        System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
        Socket server = serverSocket.accept();
        System.out.println("Just connected to " + server.getRemoteSocketAddress());
        PrintWriter out = new PrintWriter(server.getOutputStream(), true);


        try {
            Thread generator = new DataGenerator(conf, out);
            generator.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



}

class StartFeedSockets extends Thread {
    private int port;
    private HashMap conf;
    private Double partition;
    public StartFeedSockets(Integer port, HashMap conf, Double partition) {
        this.port = port;
        this.conf = conf;
        this.partition = partition;
    }

    public void run() {
        DataGeneratorHelper.execute(conf, port, partition);
    }
}


