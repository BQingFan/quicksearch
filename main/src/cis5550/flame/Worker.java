package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.flame.FlameContext.RowToString;
import cis5550.flame.FlamePairRDD.PairToPairIterable;
import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.flame.FlameRDD.StringToBoolean;
import cis5550.flame.FlameRDD.StringToIterable;
import cis5550.flame.FlameRDD.StringToPair;
import cis5550.flame.FlameRDD.StringToPairIterable;
import cis5550.kvs.*;
import cis5550.webserver.Request;

class Worker extends cis5550.generic.Worker {

    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <masterIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, ""+port, "w2");
        final File myJAR = new File("__worker"+port+"-current.jar");

        port(port);

        post("/useJAR", (request,response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        post("/rdd/flatMap", (req, res) -> {
            String oldTable = req.queryParams("old");
            String newTable = req.queryParams("new");
            String jarName = req.queryParams("jarName");
            String uniq = req.queryParams("fromKey") != null? req.queryParams("fromKey") : "null";
            StringToIterable lambda = (StringToIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));
            String kvsURL = req.queryParams("kvs");
            KVSClient kvs = new KVSClient(kvsURL);
            Iterator<Row> rows = getRows(oldTable, req, kvs);
            int i = 0;
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                Iterable<String> ret = lambda.op(value);
                if (ret != null) {
                    Iterator<String> iter = ret.iterator();
                    while(iter.hasNext()) {
                        String s = iter.next();
                        kvs.put(newTable, row.key() + uniq + i, "value", s);
                        i += 1;
                    }
                }

            }


            return ""; 
        });

        post("/rdd/mapToPair", (req, res) -> {
            String oldTable = req.queryParams("old");
            String newTable = req.queryParams("new");
            String jarName = req.queryParams("jarName"); 
            StringToPair lambda = (StringToPair) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName)); //Confirm
            String kvsURL = req.queryParams("kvs");
            KVSClient kvs = new KVSClient(kvsURL);

            Iterator<Row> rows = getRows(oldTable, req, kvs);
            while (rows.hasNext()) {
                Row row = rows.next();
                String value = row.get("value");
                FlamePair ret = lambda.op(value);
                if (ret != null) {
                    String key = ret.a;
                    String val = ret.b;
                    kvs.put(newTable, key, row.key(), val);
                }
            }


            return ""; 
        });

        post("/prdd/foldByKey", (req, res) -> {
            String newTable = req.queryParams("new");
            String oldTable = req.queryParams("old");
            String jarName = req.queryParams("jarName");
            String initial = req.queryParams("initial");

            String kvsURL = req.queryParams("kvs");

            KVSClient kvs = new KVSClient(kvsURL);
            TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));

            Iterator<Row> rows = getRows(oldTable, req, kvs);
            Map<String, String> map = new HashMap<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                String key = row.key();
                if (!map.containsKey(key)) {
                    map.put(key, initial);
                }
                for (String col: row.columns()) {
                    String value = row.get(col);
                    String ret = lambda.op(map.get(key), value);
                    if (ret != null) {
                        map.put(key, ret);
                    }
                }

            }
            for (Map.Entry<String, String> entry: map.entrySet()) {
                kvs.put(newTable, entry.getKey(), "value", entry.getValue());
            }

            return ""; 
        });

        post("/rdd/flatMapToPair", (req, res) -> {
            String newTable = req.queryParams("new");
            String oldTable = req.queryParams("old");
            String jarName = req.queryParams("jarName");
            String kvsURL = req.queryParams("kvs");

            StringToPairIterable lambda = (StringToPairIterable)Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));
            KVSClient kvs = new KVSClient(kvsURL);
            int i = 0;
            Iterator<Row> rows = getRows(oldTable, req, kvs);
            while(rows.hasNext()) {
                Row row = rows.next();
                Iterable<FlamePair> iterable = lambda.op(row.get("value"));
                if (iterable != null) {
                    Iterator<FlamePair> iter = iterable.iterator();
                    while(iter.hasNext()) {
                        FlamePair pair = iter.next();
                        kvs.put(newTable, pair.a, row.key() + i, pair.b);
                        i += 1;
                    }
                }
            }

            return "";
        });

        post("/prdd/flatMap", (req, res) -> {
            String newTable = req.queryParams("new");
            String oldTable = req.queryParams("old");
            String jarName = req.queryParams("jarName");
            String kvsURL = req.queryParams("kvs");

            KVSClient kvs = new KVSClient(kvsURL);

            PairToStringIterable lambda = (PairToStringIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));
            Iterator<Row> rows = getRows(oldTable, req, kvs);
            int i = 0;
            while(rows.hasNext()) {
                Row row = rows.next();
                for(String col: row.columns()) {
                    String val = row.get(col);
                    Iterable<String> iterable = lambda.op(new FlamePair(row.key(), val));
                    if (iterable != null) {
                        Iterator<String> iter = iterable.iterator();
                        while(iter.hasNext()) {
                            String v = iter.next();
                            kvs.put(newTable, row.key() + col + i, "value", v);
                            i += 1;
                        }
                    }
                }
            }
            return ""; 
        });

        post("/prdd/flatMapToPair", (req, res) -> {
            String newTable = req.queryParams("new");
            String oldTable = req.queryParams("old");
            String jarName = req.queryParams("jarName");
            String kvsURL = req.queryParams("kvs");

            KVSClient kvs = new KVSClient(kvsURL);

            PairToPairIterable lambda = (PairToPairIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));
            Iterator<Row> rows = getRows(oldTable, req, kvs);
            int i = 0;
            while(rows.hasNext()) {
                Row row = rows.next();
                for(String col: row.columns()) {
                    String val = row.get(col);
                    Iterable<FlamePair> iterable = lambda.op(new FlamePair(row.key(), val));
                    if (iterable != null) {
                        Iterator<FlamePair> iter = iterable.iterator();
                        while(iter.hasNext()) {
                            FlamePair v = iter.next();
                            kvs.put(newTable, v.a, row.key() + col + i, v.b);
                            i += 1;
                        }
                    }
                }
            }
            return ""; 
        });

        post("/prdd/join", (req, res) -> {
            String oldTable1 = req.queryParams("old");
            String oldTable2 = req.queryParams("old2");
            String newTable = req.queryParams("new");

            String kvsURL = req.queryParams("kvs");
            KVSClient kvs = new KVSClient(kvsURL);

            Iterator<Row> rows = getRows(oldTable1, req, kvs);

            while(rows.hasNext()) {
                Row row = rows.next();
                String key = row.key();
                Row row2 = kvs.getRow(oldTable2, key);
                if (row2 != null) {
                    for(String col: row.columns()) {
                        String val1 = row.get(col);
                        for (String col2: row2.columns()) {
                            String val2 = row2.get(col2);
                            kvs.put(newTable, key, Hasher.hash(col + "%" + col2), val1 + "," + val2);
                        }
                    }
                }
            }

            return ""; 
        });

        post("/rdd/fold", (req, res) -> {
            String initial = req.queryParams("initial");
            String jarName = req.queryParams("jarName");
            String oldTable = req.queryParams("old");
            String kvsURL = req.queryParams("kvs");
            KVSClient kvs = new KVSClient(kvsURL);
            TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));
            Iterator<Row> rows = getRows(oldTable, req, kvs);
            String ret = initial;
            while(rows.hasNext()) {
                Row row = rows.next();
                ret = lambda.op(ret, row.get("value"));
            }
            return ret; 
        });

        post("/rdd/fromTable", (req, res) -> {

            String newTable = req.queryParams("new");
            String oldTable = req.queryParams("old");
            String jarName = req.queryParams("jarName");
            String kvsURL = req.queryParams("kvs");

            KVSClient kvs = new KVSClient(kvsURL);

            RowToString lambda = (RowToString) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));
            Iterator<Row> rows = getRows(oldTable, req, kvs);

            while(rows.hasNext()) {
                Row row = rows.next();
                String ret = lambda.op(row);
                if (ret != null) {
                    kvs.put(newTable, row.key(), "value", ret);
                }
            }
            return "";
        });

        post("/rdd/filter", (req, res) -> {
            String newTable = req.queryParams("new");
            String oldTable = req.queryParams("old");
            String jarName = req.queryParams("jarName");
            String kvsURL = req.queryParams("kvs");

            KVSClient kvs = new KVSClient(kvsURL);
            Iterator<Row> rows = getRows(oldTable, req, kvs);
            StringToBoolean lambda = (StringToBoolean) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));
            
            while(rows.hasNext()) {
                Row row = rows.next();
                boolean ret = lambda.op(row.get("value"));
                if (ret) {
                    kvs.put(newTable, row.key(), "value", row.get("value"));
                }
            }
            
            return ""; 
        });

    }

    private static Iterator<Row> getRows(String oldTable, Request req, KVSClient kvs) throws Exception {
        String fromKey = req.queryParams("fromKey");
        String toKey = req.queryParams("toKey");

        Iterator<Row> rows = kvs.scan(oldTable, fromKey, toKey);
        return rows;
    }
}
