package cis5550.flame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {

    String tableName;
    FlameContextImpl fc;
    
    public FlameRDDImpl(String tableName, FlameContextImpl fc) {
        this.tableName = tableName;
        this.fc = fc;
    }
    
    @Override
    public List<String> collect() throws Exception {
        KVSClient kvs = fc.getKVS();
        List<String> rows = new ArrayList<>();
        Iterator<Row> iter = kvs.scan(tableName);
        while(iter.hasNext()) {
            rows.add(iter.next().get("value"));
        }
        return rows;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String res = fc.invokeOperation(tableName, "/rdd/flatMap", fc.jarName, Serializer.objectToByteArray(lambda));
        if (res.contains("ERROR")) {
            throw new Exception(res);
        }
        return new FlameRDDImpl(res, fc);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String res = fc.invokeOperation(tableName, "/rdd/mapToPair", fc.jarName, Serializer.objectToByteArray(lambda));
        if (res.contains("ERROR")) {
            throw new Exception(res);
        }
        return new FlamePairRDDImpl(res, fc);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int count() throws Exception {
        return fc.getKVS().count(tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        fc.getKVS().rename(tableName, tableNameArg);
        this.tableName = tableNameArg;
        
    }

    @Override
    public FlameRDD distinct() throws Exception {
        KVSClient kvs = fc.getKVS();
        Iterator<Row> rows = kvs.scan(tableName);
        String id = System.currentTimeMillis() + "" + Master.nextJobID;
        Master.nextJobID++;
        
        while(rows.hasNext()) {
            Row row = rows.next();
            String v = row.get("value");
            kvs.put(id, v, "value", v);
        }
        
        return new FlameRDDImpl(id, fc);
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        KVSClient kvs = fc.getKVS();
        Iterator<Row> rows = kvs.scan(tableName);
        Vector<String> ret = new Vector<>();
        int i = 0;
        while(rows.hasNext() && i < num) {
            Row row = rows.next();
            ret.add(row.get("value"));
            i += 1;
        }
        return ret;
    }

    @Override
    public String fold(String zeroElement, TwoStringsToString lambda) throws Exception {
        List<String> res = fc.fold(tableName, fc.jarName, zeroElement, Serializer.objectToByteArray(lambda));
        
        if (res == null || res.isEmpty()) {
            throw new Exception("No Response");
        } else if (res.get(0).contains("ERROR:")) {
            throw new Exception(res.get(0));
        }
        String response = zeroElement;
        for (String r: res) {
            response = lambda.op(response, r);
        }
        return response;
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String res = fc.invokeOperation(tableName, "/rdd/flatMapToPair", fc.jarName, Serializer.objectToByteArray(lambda));
        if (res.contains("ERROR")) {
            throw new Exception(res);
        }
        return new FlamePairRDDImpl(res, fc);
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        String res = fc.invokeOperation(tableName, "/rdd/filter", fc.jarName, Serializer.objectToByteArray(lambda));
        if (res.contains("ERROR")) {
            throw new Exception(res);
        }
        return new FlameRDDImpl(res, fc);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

}
