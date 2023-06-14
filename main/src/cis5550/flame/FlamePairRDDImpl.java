package cis5550.flame;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
    
    
    String tableName;
    FlameContextImpl fc;
    
    public FlamePairRDDImpl(String tableName, FlameContextImpl fc) {
        this.tableName = tableName;
        this.fc = fc;
    }
    
    @Override
    public List<FlamePair> collect() throws Exception {
        KVSClient kvs = Master.kvs;
        List<FlamePair> rows = new ArrayList<>();
        Iterator<Row> iter = kvs.scan(tableName);
        while(iter.hasNext()) {
            Row r = iter.next();
            String key = r.key();
            for (String col: r.columns()) {
                String val = r.get(col);
                rows.add(new FlamePair(key, val));
            }
        }
        return rows;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        String res = fc.invokeOperation(tableName, "/prdd/foldByKey", fc.jarName, Serializer.objectToByteArray(lambda), zeroElement);
        if (res.contains("ERROR")) {
            throw new Exception(res);
        }
        return new FlamePairRDDImpl(res, fc);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        fc.getKVS().rename(tableName, tableNameArg);
        this.tableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String res = fc.invokeOperation(tableName, "/prdd/flatMap", fc.jarName, Serializer.objectToByteArray(lambda));
        if (res.contains("ERROR")) {
            throw new Exception(res);
        }
        return new FlameRDDImpl(res, fc);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String res = fc.invokeOperation(tableName, "/prdd/flatMapToPair", fc.jarName, Serializer.objectToByteArray(lambda));
        if (res.contains("ERROR")) {
            throw new Exception(res);
        }
        return new FlamePairRDDImpl(res, fc);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        String t2 = ((FlamePairRDDImpl)other).tableName;
        String res = fc.invokeOperation(tableName, t2, "/prdd/join", fc.jarName);
        if (res.contains("ERROR")) {
            throw new Exception(res);
        }
        return new FlamePairRDDImpl(res, fc);
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}
