package trident.benchmark;


import com.esotericsoftware.yamlbeans.YamlReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hdfs.trident.HdfsState;
import org.apache.storm.hdfs.trident.HdfsStateFactory;
import org.apache.storm.hdfs.trident.HdfsUpdater;
import org.apache.storm.hdfs.trident.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.trident.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.trident.format.FileNameFormat;
import org.apache.storm.hdfs.trident.format.RecordFormat;
import org.apache.storm.hdfs.trident.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.trident.rotation.FileSizeRotationPolicy;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.*;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by jeka01 on 15/09/16.
 */
public class TridentBenchmark {

    public static void main(String[] args) throws Exception {
        String confPath = args[0];
        String runningMode = args[1];
        YamlReader reader = new YamlReader(new FileReader(confPath));
        Object object = reader.read();
        HashMap commonConfig = (HashMap) object;


        Config conf = new Config();
        String usecase = commonConfig.get("benchmarking.usecase").toString();
        if (runningMode.equals("cluster")) {
            if (usecase.equals("KeyedWindowedAggregation"))
                StormSubmitter.submitTopologyWithProgressBar(args[2], conf, keyedWindowedAggregation(commonConfig));
            else if(usecase.equals("WindowedJoin"))
                StormSubmitter.submitTopologyWithProgressBar(args[2], conf, windowedJoin(commonConfig));

        } else {
            LocalCluster cluster = new LocalCluster();
            if (usecase.equals("KeyedWindowedAggregation"))
                cluster.submitTopology(args[2], conf, keyedWindowedAggregation(commonConfig));
            else if(usecase.equals("WindowedJoin"))
                cluster.submitTopology(args[2], conf, windowedJoin(commonConfig));

        }
    }


    private static StormTopology windowedJoin(HashMap commonConfig) throws Exception {
        Integer port = new Integer(commonConfig.get("datasourcesocket.port").toString());
        ArrayList<String> hosts = (ArrayList<String>) commonConfig.get("datasourcesocket.hosts");
        int tridentBatchSize = new Integer(commonConfig.get("trident.batchsize").toString());
        String hdfsUrl = commonConfig.get("output.hdfs.url").toString();
        String outputPath = commonConfig.get("trident.output").toString();
        Float fileRotationSize = Float.parseFloat(commonConfig.get("file.rotation.size").toString());
        int parallelism = new Integer(commonConfig.get("max.partitions").toString());
        int slideWindowLength = new Integer(commonConfig.get("slidingwindow.length").toString());
        int slideWindowSlide = new Integer(commonConfig.get("slidingwindow.slide").toString());
        TridentTopology topology = new TridentTopology();
        Fields hdfsFields = new Fields("geo", "ts", "max_price", "min_price", "window_elements");
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(outputPath)
                .withPrefix("trident")
                .withExtension(".csv");

        RecordFormat recordFormat = new DelimitedRecordFormat()
                .withFields(hdfsFields);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(fileRotationSize, FileSizeRotationPolicy.Units.KB);

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl(hdfsUrl);

        StateFactory factory = new HdfsStateFactory().withOptions(options);

        ArrayList<Stream> joinStreamLeft = new ArrayList<>();
        ArrayList<Stream> joinStreamRight = new ArrayList<>();

        for (int i = 0; i < hosts.size() / 2; i++) {
            String host = hosts.get(i);
            IBatchSpout spout = new SocketBatchSpout(tridentBatchSize, host, port);
            Stream socketStream_i = topology.newStream("aggregation" + i, spout);
            joinStreamLeft.add(socketStream_i);
        }
        for (int i = hosts.size() / 2; i < hosts.size(); i++) {
            String host = hosts.get(i);
            IBatchSpout spout = new SocketBatchSpout(tridentBatchSize, host, port);
            Stream socketStream_i = topology.newStream("aggregation" + i, spout);
            joinStreamRight.add(socketStream_i);
        }

        topology.merge(joinStreamLeft)
                .each(new Fields("json"), new SelectFields(), new Fields("geo", "ts", "max_price", "min_price"))
                .slidingWindow(new BaseWindowedBolt.Duration(slideWindowLength, TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(slideWindowSlide, TimeUnit.MILLISECONDS),
                        new InMemoryWindowsStoreFactory(),
                        new Fields("geo", "ts", "max_price", "min_price"),
                        new MinMaxAggregator(),
                        new Fields("geo", "ts", "max_price", "min_price", "window_elements"))
        .partitionPersist(new MemoryMapState.Factory(),
                new Fields("geo", "ts", "max_price", "min_price", "window_elements"),
                new WindowUpdater());


        return topology.build();

    }

    private static StormTopology keyedWindowedAggregation(HashMap commonConfig) throws Exception {
        Integer port = new Integer(commonConfig.get("datasourcesocket.port").toString());
        ArrayList<String> hosts = (ArrayList<String>) commonConfig.get("datasourcesocket.hosts");
        int tridentBatchSize = new Integer(commonConfig.get("trident.batchsize").toString());
        String hdfsUrl = commonConfig.get("output.hdfs.url").toString();
        String outputPath = commonConfig.get("trident.output").toString();
        Float fileRotationSize = Float.parseFloat(commonConfig.get("file.rotation.size").toString());
        int parallelism = new Integer(commonConfig.get("max.partitions").toString());
        int slideWindowLength = new Integer(commonConfig.get("slidingwindow.length").toString());
        int slideWindowSlide = new Integer(commonConfig.get("slidingwindow.slide").toString());


        TridentTopology topology = new TridentTopology();
        Fields hdfsFields = new Fields("geo", "ts", "max_price", "min_price", "window_elements");
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath(outputPath)
                .withPrefix("trident")
                .withExtension(".csv");

        RecordFormat recordFormat = new DelimitedRecordFormat()
                .withFields(hdfsFields);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(fileRotationSize, FileSizeRotationPolicy.Units.KB);

        HdfsState.Options options = new HdfsState.HdfsFileOptions()
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withFsUrl(hdfsUrl);

        StateFactory factory = new HdfsStateFactory().withOptions(options);

        ArrayList<Stream> streams = new ArrayList<>();
        for (int i = 0; i < hosts.size(); i++) {
            String host = hosts.get(i);
            IBatchSpout spout = new SocketBatchSpout(tridentBatchSize, host, port);
            Stream socketStream_i = topology.newStream("aggregation" + i, spout);
            streams.add(socketStream_i);
        }


        TridentState countState =
                topology
                        .merge(streams)
                        .each(new Fields("json"), new SelectFields(), new Fields("geo", "ts", "max_price", "min_price"))
                        .partitionBy(new Fields("geo")).parallelismHint(160)
                        .slidingWindow(new BaseWindowedBolt.Duration(slideWindowLength, TimeUnit.MILLISECONDS),
                                new BaseWindowedBolt.Duration(slideWindowSlide, TimeUnit.MILLISECONDS),
                                new InMemoryWindowsStoreFactory(),
                                new Fields("geo", "ts", "max_price", "min_price"),
                                new MinMaxAggregator(),
                                new Fields("geo", "ts", "max_price", "min_price", "window_elements"))
                        .map(new FinalTS())
                        .filter(new BaseFilter() {
                            @Override
                            public boolean isKeep(TridentTuple t) {
                                return t.getDouble(2) > 0 && t.getDouble(3) > 0;
                            }
                        })
                        .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());
        return topology.build();
    }


}

@SuppressWarnings("serial")
class SelectFields extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        JSONObject obj = new JSONObject(tuple.getString(0));
        String geo = obj.getJSONObject("t").getString("geo");
        Double price = obj.getJSONObject("m").getDouble("price");
        Long ts = obj.has("ts") ? obj.getLong("ts") : System.currentTimeMillis();
       // System.out.println(obj.has("ts"));
        collector.emit(new Values(
                geo,
                ts,
                price,
                price
        ));
    }
}


class MinMaxAggregator extends BaseAggregator<Map<String, State>> {
    @Override
    public Map<String, State> init(Object o, TridentCollector tridentCollector) {
        return new HashMap<String, State>();
    }

    @Override
    public void aggregate(Map<String, State> partitionState, TridentTuple tuple, TridentCollector tridentCollector) {

        String partition = tuple.getString(0);

        State state = partitionState.get(partition);
        if (state == null) {
            state = new State();
        }

        Double maxPrice = tuple.getDouble(2);
        Double minPrice = tuple.getDouble(3);
        Long ts = tuple.getLong(1);
        if (state.ts < ts) {
            state.ts = ts;
        }
        state.window_elements = state.window_elements + 1;
        state.max = Math.max(state.max, maxPrice);
        state.min = Math.min(state.min, minPrice);
        partitionState.put(partition, state);
    }

    @Override
    public void complete(Map<String, State> partitionState, TridentCollector tridentCollector) {
        for (String partition : partitionState.keySet()) {
            State state = partitionState.get(partition);
           // System.out.println(partition + " - " + state.ts + " - " + state.max + " - " + state.min + " - " + state.window_elements);
            tridentCollector.emit(new Values(partition, state.ts, state.max, state.min, state.window_elements));
        }

    }

}

class State {
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;
    long ts = 0;
    long window_elements = 0L;
}

class MyFileOutputter implements MapFunction {
    private File file;

    public MyFileOutputter(String outputDir) {
        try {
            String uniqueID = UUID.randomUUID().toString();
            file = new File(outputDir + uniqueID + ".csv");
            file.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public Values execute(TridentTuple t) {
        try {
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(t.getString(0) + "," + t.getLong(1) + "," + t.getDouble(2) + "," + t.getDouble(3));
            fileWriter.flush();
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Values(t);
    }
}

class FinalTS implements MapFunction {

    @Override
    public Values execute(TridentTuple tuple) {
        Long ts = tuple.getLong(1);
        Long difference = System.currentTimeMillis() - ts;
        return new Values(
                tuple.getString(0),
                difference,
                tuple.getDouble(2),
                tuple.getDouble(3),
                tuple.getLong(4)
        );

    }
}

class WindowUpdater extends BaseStateUpdater<MemoryMapState<TridentTuple>> {


    @Override
    public void updateState(MemoryMapState<TridentTuple> mapState, List<TridentTuple> list, TridentCollector tridentCollector) {
        List<Object> geos = new ArrayList<Object>();
        for(TridentTuple tp : list){
            geos.add(tp.getString(0));
            //System.out.println(tp.getString(0));
        }
        Iterator dd =  mapState.getTuples();
        mapState.multiPut(Arrays.asList(geos),list);

    }
}
