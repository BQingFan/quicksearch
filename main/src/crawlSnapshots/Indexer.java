package crawlSnapshots;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class Indexer {

	public static void run(FlameContext flContext, String[] args) throws Exception {
		
		KVSClient kvs = flContext.getKVS();
		
		
		//step 1
		FlamePairRDD crawlPairs = flContext.fromTable("crawl", row -> row.get("url")+","+row.get("page")).mapToPair(s -> new FlamePair(s.substring(0,s.indexOf(',')),s.substring(s.indexOf(',')+1)));
		double N = 1;
		//step 2
		crawlPairs = crawlPairs.flatMapToPair(pair -> Arrays.asList(invertIndex(pair))).foldByKey("", (a,b) -> a.length() > 0 ? a+","+b : a+b);
		crawlPairs.saveAsTable("index");
		
		FlamePairRDD df = crawlPairs.foldByKey("0", (s1, s2) -> {
            String[] urls = s2.split(",");
            double num = Math.log(N / urls.length);
            return num + "";
        });
        
        df.saveAsTable("df");

		crawlPairs.foldByKey("0", (s1, s2) -> {
			String[] urls = s2.split(",");
			return "";
		});
	
		flContext.output("OK");
	}
		
	
	//helper function to create a pair for all unique words in a URL body (step 2)
	public static FlamePair[] invertIndex(FlamePair p) {
		String url = p._1();
		
		String[] body = p._2().split("\n"); //split body by newline character to get each individual line
		ArrayList<String> words = new ArrayList<String>();

	
		for(int i = 0; i < body.length; i++) {
			
			//remove any HTML tags in the line with a regular expression, then to lowercase
			String b = body[i].replaceAll("\\<.*?\\>", " ").toLowerCase();
			//then filter the line
			b = b.replaceAll("[\\r\\n\\t]+", " "); //replace any CR, LF, or tab characters w/ space
			b = b.replaceAll("[.,:;!?\'\"\\(\\)-]+", " "); //replace punctuation marks w/ space
			String[] bWords = b.split("\\s+"); //now split by whitespaces to get each individual word into array
			
			//now we have a list of words, and we just want to add the unique ones to the arraylist
			for(int j = 0; j < bWords.length; j++) {
				if(bWords[j].length() > 0 && !words.contains(bWords[j])) {
					words.add(bWords[j]);
				}
			}
			
			
		}
		
		//now return as a FlamePair[]
		FlamePair[] output = new FlamePair[words.size()];
		for(int i = 0; i < output.length; i++) {
			output[i] = new FlamePair(words.get(i),url);
		}
		
		return output;
	}
	
}
