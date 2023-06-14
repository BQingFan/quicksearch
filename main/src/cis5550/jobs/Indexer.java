package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.*;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class Indexer {

	public static void run(FlameContext flContext, String[] args) throws Exception {
		
		flContext.output("Start");
		KVSClient kvs = flContext.getKVS();
        String kvsMaster = kvs.getMaster();
        
        flContext.getKVS().persist("df");
	    flContext.getKVS().persist("index");
        flContext.getKVS().persist("tf");
	    
        //step 1
        FlameRDD crawlTable = flContext.fromTable("crawl", row -> row.get("url")+","+row.get("page"));
        FlamePairRDD crawlPairs = crawlTable.mapToPair(s -> new FlamePair(s.substring(0,s.indexOf(',')),s.substring(s.indexOf(',')+1)));
        
        double N = crawlTable.count();
        //step 2
        crawlPairs = crawlPairs.flatMapToPair(pair -> Arrays.asList(invertIndex(pair, kvsMaster))).foldByKey("", (a,b) -> a.length() > 0 ? a+","+b : a+b).flatMapToPair(pair -> Arrays.asList(urlOrdering(pair)));
        List<FlamePair> toIndex = crawlPairs.collect();
        for(FlamePair p : toIndex) {
        	Row r = new Row(p._1());
        	r.put(p._1(), p._2());
        	kvs.putRow("index", r);
        }
        
        
        FlamePairRDD df = crawlPairs.foldByKey("0", (s1, s2) -> {
            String[] urls = s2.split(",");
            double num = Math.log(N / urls.length);
            return num + "";
        });
        
        List<FlamePair> toDF = df.collect();
        for(FlamePair p : toDF) {
        	Row r = new Row(p._1());
        	r.put(p._1(), p._2());
        	kvs.putRow("df", r);
        }
        
        
        
        
        flContext.output("OK");
	}
		
	
	//helper function to create a pair for all unique words in a URL body (step 2)
	public static FlamePair[] invertIndex(FlamePair p, String kvsString) throws Exception {
		KVSClient kvs = new KVSClient(kvsString);
		String url = p._1();
		Row row = new Row(url);
		String[] body = p._2().split("\n"); //split body by newline character to get each individual line
		HashMap<String, ArrayList<Integer>> words = new HashMap<String, ArrayList<Integer>>();
		int wordPos = 0;
	
		for(int i = 0; i < body.length; i++) {
			
			//remove any HTML tags in the line with a regular expression, then to lowercase
			String b = body[i].replaceAll("\\<.*?\\>", " ").toLowerCase();
			//then filter the line
			b = b.replaceAll("[\\r\\n\\t]+", " "); //replace any CR, LF, or tab characters w/ space
			b = b.replaceAll("[.,:;!?\'\"\\(\\)-]+", " "); //replace punctuation marks w/ space
			String[] bWords = b.split("\\s+"); //now split by whitespaces to get each individual word into array
			
			//filter the array to remove any "non-word" strings (now can only contain letters & numbers)
			bWords = Arrays.stream(bWords).filter(word -> word.matches("[a-zA-Z0-9]+")).toArray(String[]::new);
			
			//now we have a list of words, and we just want to add the unique ones to the arraylist
			for(int j = 0; j < bWords.length; j++) {
				if(bWords[j].length() > 0) {
					ArrayList<Integer> posList;
					if(!words.containsKey(bWords[j])) {
						posList = new ArrayList<>();
						posList.add(wordPos);
						words.put(bWords[j], posList);
					} else {
						posList = words.get(bWords[j]);
						posList.add(wordPos);
					}
					if (!bWords[j].contains("[^a-zA-Z\\-0-9]")) {
						byte[] x = bWords[j].getBytes();
						bWords[j] = new String(x);
						System.out.println(bWords[j]);
                        row.put(bWords[j], (posList.size() + "").getBytes());
                    }
				}
				wordPos++;
			}
			
			
		}
//		System.out.println("row: "+row);
		kvs.putRow("tf", row);
		//now return as a FlamePair[]
		FlamePair[] output = new FlamePair[words.size()];
		int i = 0;
		for(Entry<String, ArrayList<Integer>> entry : words.entrySet()) {
			String word = entry.getKey();
			String urlWithPositions = url + ":";
			ArrayList<Integer> posList = entry.getValue();
			for(int j = 0; j < posList.size(); j++) {
				if(j == 0) {
					urlWithPositions += posList.get(j);
				} else {
					urlWithPositions += " "+posList.get(j);
				}
			}
			output[i] = new FlamePair(word, urlWithPositions);
			i++;
		}
		
		return output;
	}
	
	public static FlamePair[] urlOrdering(FlamePair p) {
		String[] urls = p._2().split(",");
		HashMap<Integer, String> vals = new HashMap<>();
		
		//place all vals in hashmap (key=count, val=list of urls)
		for(int i = 0; i < urls.length; i++) {
			String currSpl[] = urls[i].split(":");
			int count = currSpl[currSpl.length-1].split(" ").length;
			if(vals.containsKey(count)) {
				vals.put(count, vals.get(count)+","+urls[i]);
			} else {
				vals.put(count, urls[i]);
			}
		}
		
		//now sort by keys into descending order and build output string
		String outputURLs = "";
		SortedSet<Integer> keySet = new TreeSet<>(vals.keySet()); //should sort?
		while(!keySet.isEmpty()) {
			int x = keySet.last();
			if(outputURLs.length() == 0) {
				outputURLs += vals.get(x);
			} else {
				outputURLs += ","+vals.get(x);
			}
			keySet.remove(x);
		}
		
		return new FlamePair[] {new FlamePair(p._1(), outputURLs)};
		
	}
	
}
