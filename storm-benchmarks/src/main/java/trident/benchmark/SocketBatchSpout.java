package trident.benchmark;

/**
 * Created by jeka01 on 15/09/16.
 */

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"serial", "rawtypes"})
public class SocketBatchSpout implements IBatchSpout {
    private static final AtomicInteger seq = new AtomicInteger();
    private Socket clientSocket;
    private int port;
    private String hostname;
    private BufferedReader in;
    private int batchSize;


    public SocketBatchSpout(int batchSize, String hostname,int port) throws IOException {
        this.batchSize = batchSize;
        this.port = port;
        this.hostname =  hostname;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open(Map conf, TopologyContext context) {
        // init
       // System.err.println(this.getClass().getSimpleName() +":" + seq.incrementAndGet()+" opened.");
        try {
            this.clientSocket = new Socket(hostname , port);
            InputStream inFromServer = clientSocket.getInputStream();
            DataInputStream reader = new DataInputStream(inFromServer);
            in = new BufferedReader(new InputStreamReader(reader, "UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        // emit batchSize fake tweets
        for (int i = 0; i < batchSize; i++) {
            try {
                String jsonStr = in.readLine();
                collector.emit(new Values(jsonStr));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void ack(long batchId) {
        // nothing to do here
    }

    @Override
    public void close() {
        // nothing to do here
    }

    @Override
    public Map getComponentConfiguration() {
        // no particular configuration here
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("json");
    }}

