package trident.benchmark;


import com.esotericsoftware.yamlbeans.YamlReader;
import data.source.socket.DataGenerator;
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
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.JSONObject;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by jeka01 on 15/09/16.
 */
public class TridentBenchmark {

    public static void main(String[] args) throws Exception {
//        if (args.length != 2) {
  //          throw new Exception("2 arguments is needed: Config file path and running mode (local or cluster)");
    //    }
        String confPath = args[0];
        String runningMode = args[1];
        YamlReader reader = new YamlReader(new FileReader(confPath));
        Object object = reader.read();
        Map commonConfig = (Map) object;

       // int workers = new Integer(commonConfig.get("storm.workers").toString());
//        int ackers = new Integer(commonConfig.get("storm.ackers").toString());
  //      int cores = new Integer(commonConfig.get("process.cores").toString());
        int parallelism = new Integer(commonConfig.get("parallelism.default").toString());
        int slideWindowLength = new Integer(commonConfig.get("slidingwindow.length").toString());
        int slideWindowSlide = new Integer(commonConfig.get("slidingwindow.slide").toString());

        Integer dataGeneratorPort = new Integer(commonConfig.get("datasourcesocket.port").toString());
        String dataGeneratorHost = commonConfig.get("datasourcesocket.host").toString();

        Long benchmarkingCount = new Long(commonConfig.get("benchmarking.count").toString());
        Long warmupCount = new Long(commonConfig.get("warmup.count").toString());
        Long sleepTime = new Long(commonConfig.get("datagenerator.sleep").toString());
        int tridentBatchSize = new Integer(commonConfig.get("trident.batchsize").toString());
        String hdfsUrl = commonConfig.get("output.hdfs.url").toString();
        String outputPath = commonConfig.get("trident.output").toString();
	    Float fileRotationSize = Float.parseFloat(commonConfig.get("file.rotation.size").toString());
        Long blobSize = new Long(commonConfig.get("datagenerator.blobsize").toString());

        DataGenerator.generate(dataGeneratorPort, benchmarkingCount, warmupCount, sleepTime,blobSize);
        Thread.sleep(1000);


        // Storm can be run locally for testing purposes
        Config conf = new Config();
        if (runningMode.equals("cluster")) {
            //conf.setNumWorkers(workers);
            //conf.setNumAckers(workers+3);
            StormSubmitter.submitTopologyWithProgressBar(args[2], conf, keyedWindowAggregations(new SocketBatchSpout(tridentBatchSize, dataGeneratorHost , dataGeneratorPort), parallelism, slideWindowLength, slideWindowSlide, outputPath,hdfsUrl,fileRotationSize ));
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(args[2], conf, keyedWindowAggregations(new SocketBatchSpout(tridentBatchSize, dataGeneratorHost, dataGeneratorPort), parallelism, slideWindowLength, slideWindowSlide,outputPath,hdfsUrl,fileRotationSize));

        }
    }

    public static StormTopology keyedWindowAggregations(IBatchSpout spout, int parallelism, int slideWindowLength, int slideWindowSlide, String outputPath, String hdfsUrl,float fileRotationSize) throws Exception {

        // A topology is a set of streams.
        // A stream is a DAG of Spouts and Bolts.
        // (In Storm there are Spouts (data producers) and Bolts (data processors).
        // Spouts create Tuples and Bolts manipulate then and possibly emit new ones.)

        // But in Trident we operate at a higher level.
        // Bolts are created and connected automatically out of higher-level constructs.
        // Also, Spouts are "batched".
        TridentTopology topology = new TridentTopology();


        // You can perform aggregations by grouping the stream and then applying an aggregation
        // Note how each actor appears more than once. We are aggregating inside small batches (aka micro batches)
        // This is useful for pre-processing before storing the result to databases
      //  Aggregator groupedAgg = new GroupedAggregator(new MinMaxAggregator(), new Fields("geo"), new Fields("geo", "ts", "max_price", "min_price"), new Fields("geo", "ts", "max_price", "min_price").size());


        Fields hdfsFields = new Fields("geo", "ts", "max_price", "min_price");

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



        TridentState countState =
        topology
                .newStream("aggregation", spout)
                .each(new Fields("json"), new SelectFields(), new Fields("geo", "ts", "max_price", "min_price"))
                .partitionBy(new Fields("geo")).parallelismHint(160)
                .slidingWindow(new BaseWindowedBolt.Duration(slideWindowLength, TimeUnit.MILLISECONDS),
                        new BaseWindowedBolt.Duration(slideWindowSlide, TimeUnit.MILLISECONDS),
                        new InMemoryWindowsStoreFactory(),
                        new Fields("geo", "ts", "max_price", "min_price"),
                        new MinMaxAggregator(),
                        new Fields("geo", "ts", "max_price", "min_price"))
                .map(new FinalTS())
                .partitionPersist(factory, hdfsFields, new HdfsUpdater(), new Fields());





//                peek(new Consumer() {
//                    @Override
//                    public void accept(TridentTuple input) {
//
//                        System.out.println(input);
//                    }
//                });
        // .each(new Fields("geo","ts","max_price","min_price"), new Print());


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
        collector.emit(new Values(
                geo,
                System.currentTimeMillis() ,
                price,
                price
        ));
    }
}


//class MinMaxAggregator extends BaseAggregator<MinMaxAggregator.State> {
//
//    class State {
//        double max = 0.0;
//        double min = 0.0;
//        long ts = 0;
//        String id = "";
//    }
//
//    @Override
//    public State init(Object batchId, TridentCollector collector) {
//        return new State();
//    }
//
//    @Override
//    public void aggregate(State state, TridentTuple tuple, TridentCollector collector) {
//        Double maxPrice = tuple.getDouble(2);
//        Double minPrice = tuple.getDouble(3);
//        Long ts = tuple.getLong(1);
//        String id = tuple.getString(0);
//        if (state.ts < ts) {
//            state.ts = ts;
//            state.id = id;
//        }
//        state.max = Math.max(state.max, maxPrice);
//        state.min = Math.min(state.min, minPrice);
//    }
//
//    @Override
//    public void complete(State state, TridentCollector collector) {
//        collector.emit(new Values(state.id, state.ts, state.max, state.min));
//    }
//
//}


class MinMaxAggregator extends BaseAggregator<Map<String, State>> {


    @Override
    public Map<String, State> init(Object o, TridentCollector tridentCollector) {
        return new HashMap<String, State>();
    }

    @Override
    public void aggregate(Map<String, State> partitionState, TridentTuple tuple, TridentCollector tridentCollector) {
        String partition = tuple.getString(0);
        State state = partitionState.get(partition);
        if (state == null){
            state = new State();
        }

        Double maxPrice = tuple.getDouble(2);
        Double minPrice = tuple.getDouble(3);
        Long ts = tuple.getLong(1);
        String id = tuple.getString(0);
        if (state.ts < ts) {
            state.ts = ts;
            state.id = id;
        }
        state.max = Math.max(state.max, maxPrice);
        state.min = Math.min(state.min, minPrice);
        partitionState.put(partition, state);
    }

    @Override
    public void complete(Map<String, State> partitionState, TridentCollector tridentCollector) {
        for(State state: partitionState.values()){
            tridentCollector.emit(new Values(state.id, state.ts, state.max, state.min));
        }

    }
}

class State {
    double max = Double.MIN_VALUE;
    double min = Double.MAX_VALUE;
    long ts = 0;
    String id = "";
}

class MyFileOutputter implements MapFunction {
    private File file;

    public MyFileOutputter(String outputDir){
        try{
            String uniqueID = UUID.randomUUID().toString();
            file = new File(outputDir+uniqueID+".csv");
            file.createNewFile();
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public Values execute(TridentTuple t) {
        try{
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write(t.getString(0) + "," + t.getLong(1) + "," + t.getDouble(2) + "," + t.getDouble(3));
            fileWriter.flush();
            fileWriter.close();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return new Values(t);
    }
}
class FinalTS implements MapFunction {

    @Override
    public Values execute(TridentTuple tuple) {
        Long ts = tuple.getLong(1);
        Long difference = System.currentTimeMillis()  - ts;
        return new Values(
                tuple.getString(0),
                difference,
                tuple.getDouble(2),
                tuple.getDouble(3)
        );

    }
}

