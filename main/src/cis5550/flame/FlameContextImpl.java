package cis5550.flame;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.HTTP.Response;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;

public class FlameContextImpl implements FlameContext {
    
    List<String> out;
    String jarName;
    public FlameContextImpl(String jarName, List<String> out) {
        this.out = out;
        this.jarName = jarName;
    }
    
    @Override
    public KVSClient getKVS() {
        return Master.kvs;
    }

    @Override
    public void output(String s) {
        out.add(s);
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String id = System.currentTimeMillis() + "" + Master.nextJobID;
        Master.nextJobID++;
        int i = 0;
        KVSClient kvs = getKVS();
        for (String val: list) {
            kvs.put(id, Hasher.hash(i + ""), "value", val);
            i += 1;
        }
        
        return new FlameRDDImpl(id, this);
    }
    
    public List<String> fold(String tableName, String jarName, String zeroElement, byte[] lambda) throws Exception {
        String operation = "/rdd/fold?initial=" + zeroElement;
        Partitioner part = new Partitioner();
        KVSClient kvs = Master.kvs;
        String kvsURL = "&kvs=" + kvs.getMaster();
        String workersString = Master.getWorkers();
        String[] workersWithNum = workersString.split("\n");
        String[] workers = new String[workersWithNum.length - 1];
        System.arraycopy(workersWithNum, 1, workers, 0, workers.length);
        for(int i = 0; i < workers.length; i++) {
        	int splitPos = workers[i].indexOf(",");
        	workers[i] = workers[i].substring(splitPos + 1);
  	  	}
        
        
        part.addKVSWorker(workers[0], null, workers[0]);
        int kvsWorkers = getKVS().numWorkers();
        for (int i = 0; i < workers.length; i++) {
            String worker = workers[i];
            String nextWorker = null;
            if (i < kvsWorkers) {
                if (i < kvsWorkers - 1) {
                    nextWorker = workers[i + 1];
                }
                part.addKVSWorker(worker, worker, nextWorker);
            }
            part.addFlameWorker(worker);
        }
        
        Vector<Partitioner.Partition> partitions = part.assignPartitions();
        Thread[] threads = new Thread[partitions.size()];
        List<Response> responses = new ArrayList<>();
        int i = 0;
        for(Partitioner.Partition partition: partitions) {
            
            Thread thread = new Thread(() -> {
                String url = "http://" + partition.assignedFlameWorker + operation + "?old=" + tableName + "&jarName=" + jarName;
                if (operation.contains("?")) {
                    url = "http://" + partition.assignedFlameWorker + operation + "&old=" + tableName + "&jarName=" + jarName;
                }
                
                if (partition.fromKey != null) {
                    url += "&fromKey=" + partition.fromKey;
                }
                if (partition.toKeyExclusive != null) {
                    url += "&toKey=" + partition.toKeyExclusive;
                } 
                url += kvsURL;
                try {
                    
                    Response res = HTTP.doRequest("POST", url, lambda);
                    responses.add(res);
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            thread.start();
            threads[i] = thread;
            i += 1;
        }
        
        for (Thread thread: threads) {
            thread.join();
        }
        List<String> resps = new ArrayList<>();
        String ret = "";
        boolean err = false;
        for (Response res: responses) {
            int code = res.statusCode();
            if (code != 200) {
                if (!err) {
                    err = true;
                    ret = "ERRORS: " + res.statusCode() + " ";
                } else {
                    ret += res.statusCode() + " ";
                }
            } else {
                resps.add(new String(res.body()));
            }
        }
        
        if (err) {
            resps.clear();
            resps.add(ret);
        }
        return resps;
    }
    
    public String invokeOperation(String table1, String table2, String operation, String jarName) throws Exception {
        return invokeOperation(table1, operation + "?old2=" + table2, jarName, new byte[0]);
    }
    
    public String invokeOperation(String tableName, String operation, String jarName, byte[] lambda, String zeroEl) throws Exception {
        return invokeOperation(tableName, operation + "?initial=" + URLEncoder.encode(zeroEl, "UTF-8"), jarName, lambda);
    }
    public String invokeOperation(String tableName, String operation, String jarName, byte[] lambda) throws Exception {
        String id = System.currentTimeMillis() + "" + Master.nextJobID;
        Master.nextJobID++;
        Partitioner part = new Partitioner();
        KVSClient kvs = Master.kvs;
        String kvsURL = "&kvs=" + kvs.getMaster();
        
        
        String workersString = Master.getWorkers();
        String[] workersWithNum = workersString.split("\n");
        String[] workers = new String[workersWithNum.length - 1];
        System.arraycopy(workersWithNum, 1, workers, 0, workers.length);
        for(int i = 0; i < workers.length; i++) {
        	int splitPos = workers[i].indexOf(",");
        	workers[i] = workers[i].substring(splitPos + 1);
  	  	}
        part.addKVSWorker(workers[0], null, workers[0]);
        int kvsWorkers = getKVS().numWorkers();
        for (int i = 0; i < workers.length; i++) {
            String worker = workers[i];
            String nextWorker = null;
            if (i < kvsWorkers) {
                if (i < kvsWorkers - 1) {
                    nextWorker = workers[i+1];
                }
                part.addKVSWorker(worker, worker, nextWorker);
            }
            part.addFlameWorker(worker);
        }
        //part.setKeyRangesPerWorker(kvsWorkers);
        
        Vector<Partitioner.Partition> partitions = part.assignPartitions();
        Thread[] threads = new Thread[partitions.size()];
        List<Response> responses = new ArrayList<>();
        int i = 0;
        for(Partitioner.Partition partition: partitions) {
            
            Thread thread = new Thread(() -> {
                String url = "http://" + partition.assignedFlameWorker + operation + "?old=" + tableName + "&new=" + id + "&jarName=" + jarName;
                if (operation.contains("?")) {
                    url = "http://" + partition.assignedFlameWorker + operation + "&old=" + tableName + "&new=" + id + "&jarName=" + jarName;
                }
                
                if (partition.fromKey != null) {
                    url += "&fromKey=" + partition.fromKey;
                }
                if (partition.toKeyExclusive != null) {
                    url += "&toKey=" + partition.toKeyExclusive;
                } 
                url += kvsURL;
                try {
                    
                    Response res = HTTP.doRequest("POST", url, lambda);
                    responses.add(res);
                    
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });
            thread.start();
            threads[i] = thread;
            i += 1;
        }
        
        for (Thread thread: threads) {
            thread.join();
        }
        String ret = id;
        for (Response res: responses) {
            int code = res.statusCode();
            if (code != 200) {
                if (ret.equals(id)) {
                    ret = "ERRORS: " + res.statusCode() + " ";
                } else {
                    ret += res.statusCode() + " ";
                }
                
            }
        }
        return ret;
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String ret = invokeOperation(tableName, "/rdd/fromTable", this.jarName, Serializer.objectToByteArray(lambda));
        if (ret.contains("ERRORS:")) {
            throw new Exception(ret);
        }
        return new FlameRDDImpl(ret, this);
    }

    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {
        // TODO Auto-generated method stub
        
    }

}
