package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.io.IOException;
import java.util.*;

import cis5550.flame.*;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {
	
	public static void run(FlameContext flContext, String[] args) throws Exception {
		
		if(args.length != 1 ) {
			flContext.output("Error: args should only contain the threshold value");
			return;
		}
		
		Double THRESHOLD;
		try {
			THRESHOLD = Double.parseDouble(args[0]);
		} catch (Exception e){
			flContext.output("Error: args threshold value should be a double");
			return;
		}
		
	
		KVSClient kvs = flContext.getKVS();
		
		//step 3
		FlamePairRDD stateTable = flContext.fromTable("crawl", row -> row.get("url")+","+row.get("page")).mapToPair(s -> new FlamePair(s.substring(0,s.indexOf(',')),s.substring(s.indexOf(',')+1)));
		stateTable = stateTable.flatMapToPair(pair -> Arrays.asList(extractURLs(pair)));
		stateTable.saveAsTable("stateTable");
		
		System.out.println("state table generated");

		
		while(true) {
			System.out.println("starting another pagerank loop...");
			//step 4
			FlamePairRDD transferTable = stateTable.flatMapToPair(pair -> Arrays.asList(transferTableComputation(pair)));
			
			System.out.println("computes");
			
			//step 5
			transferTable = transferTable.foldByKey("0.0", (a,b) -> ""+(Double.parseDouble(a)+Double.parseDouble(b)));
			System.out.println("folds");
			
			//step 6
			stateTable = stateTable.join(transferTable).flatMapToPair(pair -> Arrays.asList(updateRankings(pair)));
			System.out.println("joins");
			
			//step 7
			if(stopLoop(stateTable, THRESHOLD)) {
				break;
			}
		}
		
		System.out.println("exits loop");
		
		//step 8
		stateTable = stateTable.flatMapToPair(pair -> Arrays.asList(saveResults(pair, flContext))); //this creates the pagerank.table...
		kvs.delete("stateTable"); //...so we can delete the state table now
		
		
		flContext.output("OK");
		
	}
	
	
	//helper function to extract all the URLs (based on HW8 function)
	public static FlamePair[] extractURLs(FlamePair p) {
		String url = p._1();
		String[] body = p._2().split("\n");
		
		
		ArrayList<String> extracted = new ArrayList<>();
		
		
		
		// <a href='http://foo.com:80/page1.html'></a><a href='http://foo.com:80/page3.html'></a>
		
		for(int i = 0; i < body.length; i++) {
			
			String line = body[i];
			
			while(line != null && line.contains("<a") && line.substring(line.indexOf("<a")).contains(">")) {
				String[] found = extractAnchor(line, url);
				if(!extracted.contains(found[1])) {
					extracted.add(found[1]);
				}
				line = found[0];
			}
		}
		
		//combine the results to make the comma-separated list of links
		String urls = "";
		for(int i = 0; i < extracted.size(); i++) {
			if(urls.length() == 0) {
				urls += extracted.get(i);
			} else {
				urls += ","+extracted.get(i);
			}
		}
		
		//return it as a FlamePair[]
		return new FlamePair[] { new FlamePair(url, "1.0,1.0,"+urls)};
	}
	
	public static String[] extractAnchor(String line, String url) {
//		System.out.println(line);
//		System.out.println("in: "+url);
		String[] output = new String[2];
		
		//<a href='http://foo.com:80/page1.html'>
		
		int begin = line.indexOf("<a");
		String extractLine = line.substring(begin); //get everything after the beginning of the anchor tag
		int end = extractLine.indexOf(">");
		extractLine = extractLine.substring(0, end); //get everything within the first anchor tag (not including closing '>')
		
		// <a href='http://foo.com:80/page1.html'
		
		output[0] = line.substring(end); //output contains everything after the < now
		// </a><a href='http://foo.com:80/page3.html'></a>

		String[] anchor = extractLine.split(" ");
		// <a | href='http://foo.com:80/page1.html'

		for(int j = 0; j < anchor.length; j++) {
			if(anchor[j].startsWith("href=")) {
				String extractedURL = anchor[j].substring(5); //remove all before first quote mark
				extractedURL = extractedURL.substring(1, extractedURL.length()-1); //then extract inside quotes

				//normalize the URL and add to list (if doesn't already exist)
				extractedURL = parseExtractedURL(extractedURL, url);
				if(extractedURL.contains("..")) {
					break;
				}
				output[1] = extractedURL;
				return output;
			}
		}
		
		//in the case that there's no href extracted (?)
		output[1] = null;
		return output;
		
	}

	
	
	//helper function to normalize and filter the extracted URLs
	public static String parseExtractedURL(String url, String baseURL) {
			
			String[] parsedBaseURL = URLParser.parseURL(baseURL);
			String[] parsedURL = URLParser.parseURL(url);
			
			//cut off the part after the # if it exists
			if(parsedURL[3].contains("#")) {
				parsedURL[3] = parsedURL[3].substring(0, parsedURL[3].indexOf("#"));
				if(parsedURL[3].length() == 0) {
					return baseURL;
				}
			}
			
			if(parsedURL[0] != null) { //if it is not a relative link
				if(parsedURL[2] == null) { //if it doesn't contain a port#, add it
					if(parsedURL[0].equals("https")) {
						parsedURL[2] = "443";
					} else {
						parsedURL[2] = "80";
					}
				}
				return parsedURL[0] + "://" + parsedURL[1] + ":" + parsedURL[2] + parsedURL[3];
				
			} else { //if it is a relative link
				
				//get the base URL (without the stuff after the last slash)
				String[] baseSpl = parsedBaseURL[3].split("/");
				String base = "/";
				if(baseSpl.length > 1) {
					base = parsedBaseURL[3].substring(0, parsedBaseURL[3].indexOf(baseSpl[baseSpl.length-1]));
				}
				
				if(!parsedURL[3].contains("/")) { //for html elements at the current path
					return parsedBaseURL[0] + "://" + parsedBaseURL[1] + ":" + parsedBaseURL[2] + base + parsedURL[3];
				}
				
				
				String[] splitPath = parsedURL[3].split("/");
				
				//this would occur if url was = "/"
				if(splitPath.length == 0) {
					return parsedBaseURL[0] + "://" + parsedBaseURL[1] + ":" + parsedBaseURL[2] + "/";
				}
				
				String newLink = "";
				
				//iterate through each part of the path and build the new link
				boolean includeBase = true; //include the base path if remains true
				for(int i = splitPath.length-1; i >= 0; i--) {
					if(splitPath[i].length() == 0) { //this occurs if the string started with a "/" ==> new path from top level directory
						includeBase = false;
						continue;
					}
					if(i == splitPath.length-1) { //if we are at last index, just add the stuff (no extra slash)
						newLink = splitPath[i];
						continue;
					}
					if(splitPath[i].equals("..")) { //if we see a "..", skip it and the path before it
						if(i == 0) { //if last index, need to skip the last path in the base path
							baseSpl = base.split("/");
							if(baseSpl.length > 0) {
								base = parsedBaseURL[3].substring(0, parsedBaseURL[3].indexOf(baseSpl[baseSpl.length-1]));
							}
							continue;
						}
						i--;
						continue;
					}
					newLink = splitPath[i] + "/" + newLink; //otherwise, add the path (starting from the back)
				}
				
				//return the base + newLink
				if(!includeBase) {
					return parsedBaseURL[0] + "://" + parsedBaseURL[1] + ":" + parsedBaseURL[2] + "/" + newLink;
				} else {
					return parsedBaseURL[0] + "://" + parsedBaseURL[1] + ":" + parsedBaseURL[2] + base + newLink;
				}
				
			}

		}
	
	

	//helper function to compute the transfer table
	public static FlamePair[] transferTableComputation(FlamePair p) {
		String url = p._1();
		String[] spl2 = p._2().split(","); //0 is Rc, 1 is Rp, and the rest are each link found in that URL
		
		double Rc = Double.parseDouble(spl2[0]);
		double Rp = Double.parseDouble(spl2[1]);
		
		ArrayList<FlamePair> pairs = new ArrayList<FlamePair>();
		
		if(spl2.length > 2) {
			//compute all the pairs
			int n = spl2.length - 2;
			double v = 0.85 * Rc / n;
			pairs.add(new FlamePair(url, "0.0"));
			
			for(int i = 2; i < spl2.length; i++) {
				pairs.add(new FlamePair(spl2[i], Double.toString(v)));
			}
			
		}
		
		FlamePair[] output = new FlamePair[pairs.size()];
		for(int i=0; i<output.length; i++) {
			output[i] = pairs.get(i);
		}
//		System.out.println("returns?");
		return output;
		
		
	}
	
	
	//helper function to update the prev and curr rankings
	public static FlamePair[] updateRankings(FlamePair p) {
		String url = p._1();
		
		// 1.0,1.0,http://simple.crawltest.cis5550.net:80/UJ2p.html,1.6089285714285713
		// vals: RankCurrent, RankPrev,url1, ... , urlN, RankNew
		// always: 0, 1, ..., RankNew ==> len-1
		String[] spl2 = p._2().split(",");
		
		String updatePrev = spl2[0]; // curr -> prev
		String updateCurr = Double.toString(Double.parseDouble(spl2[spl2.length-1])+0.15); //new -> curr
		
		String urls = "";
		for(int i=2; i<spl2.length-1; i++) {
			if(i == 2) {
				urls += spl2[i];
			} else {
				urls += "," + spl2[i];
 			}
		}
		
		return new FlamePair[] {new FlamePair(url, updateCurr+","+updatePrev+","+urls)};
	}

	//helper function to check if the main loop should be stopped
	public static boolean stopLoop(FlamePairRDD table, Double t) throws Exception {
		//compute the maximum change in ranks across all pages via flatmap
		String maxChange = table.flatMap(pair -> Arrays.asList(getDifference(pair))).fold("0.0",
				(a,b) -> 
//		Double.parseDouble(a) >= Double.parseDouble(b) ? a : b);
		{
			if(a.length() == 0) { return b; } if (b.length() == 0) { return a; }
			if(Double.parseDouble(a) >= Double.parseDouble(b)) { return a; } return b;
		});
		
		//check if below the threshold
		if(Double.parseDouble(maxChange) < t) {
			return true;
		} else {
			return false;
		}
		
		
	}
	
	//helper function to calculate the abs diff of two double values in the flame pair
	public static String[] getDifference(FlamePair p) {
		String[] vals = p._2().split(",");
		Double curr = Double.parseDouble(vals[0]);
		Double prev = Double.parseDouble(vals[1]);
		
		return new String[] { Double.toString(Math.abs(curr) - Math.abs(prev)) };
		
		
	}

	//helper function to save the results into pagerank.table after the final iteration
	public static FlamePair[] saveResults(FlamePair p, FlameContext ctxt) throws IOException {
		String url = p._1();
		String newRank = p._2().split(",")[0];
		
		KVSClient kvs = ctxt.getKVS();
		kvs.persist("pageranks");
		kvs.put("pageranks", url, "rank", newRank);
		
		
		return new FlamePair[] {}; //return an empty array
	}
	
}
