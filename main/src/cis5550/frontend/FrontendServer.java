package cis5550.frontend;

import cis5550.tools.URLParser;
import cis5550.webserver.Server;
import cis5550.kvs.*;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.net.URLDecoder;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;

public class FrontendServer {
	public static String[] header = {
			  "<html><head><title>555ogle</title>",
			  "<link rel=\"shortcut icon\" type=\"image/png\" href=\"https://google.com/favicon.ico\">",
			  "<style>\n",
			  ".suggestion-item:hover {\n",
			  "background-color: white;\n",
			  "cursor: auto;\n",
			  "}\n",
			  
			  ".error-container {\n",
			  "margin: 100px auto;\n",
			  "text-align: center;\n",
			  "}\n",
			  ".error-heading {\n",
			  "font-size: 22px;\n",
			  "color: #333;\n",
			  "margin-bottom: 18px;\n",
			  "}\n",
			  ".error-message {\n",
			  "font-size: 16px;\n",
			  "color: #666;\n",
			  "}\n",
			  
			  "</style>\n",
			  "<script src=\"https://code.jquery.com/jquery-3.6.0.min.js\"></script><script>",
			  "var page = 1;\n",
			  "var fetching = false;\n",
			  "var notend = true;\n",
			  "var TRIGGER_SCROLL_SIZE = 10;\n",
			  
			  "$(window).scroll(function() {\n",
			  	"if(!fetching && notend){\n",
			  	"var scrollTop = window.pageYOffset;\n",
			  	"console.log('scrollTop: '+scrollTop);\n",
			  	"console.log('height: '+$(window).height());\n",
			  	"console.log('doc height: '+$(document).height());\n",
			  	"if(Math.abs(scrollTop + 800 - $(window).height()) <= TRIGGER_SCROLL_SIZE) {\n",
			  		"fetching = true;\n",
			  		"page++;\n",
			  		"console.log('scrolled to the end');\n",
			  		"loadSearchResults();\n",
			  		"fetching = false;\n",
			  	"}\n",
			   "}\n",
			  "});\n",
			  
			 
			 
			 
			 
			 "function cleanUp(query) {\n",
			 "query = query.trim();\n",
			 "query = query.replace(/\\s+/g, ' ');\n",
			 "return query;\n",
			 "}\n",
			 "",
			 "",

			  
			  "function loadSearchResults() {\n",
			  "const queryString = window.location.search;\n",
			  	"const urlParams = new URLSearchParams(queryString);\n",
			  	"const query = urlParams.get('query');\n",
				  "console.log('MAD IT');\n",
			  "$.ajax({\n",
			  	"url: '/scroll',\n",
			  	"dataType: 'json',\n",
			  	"data: { page: page, query: query },\n",
			  	"success: function(response) {\n",
			  	"console.log('query: '+query);\n",
			  	"var results = response.results;\n",
			  	"if (results.length == 0) {\n",
			  	"notend = false;\n",
			  	"} else {\n",
			  		"console.log(results);\n",
			  		"console.log(typeof results);\n",
			  		"var resultsDiv = document.getElementById('results');\n",
			  		"for(var i=0; i<results.length; i++){\n",
			  			"var result = JSON.parse(results[i]);\n",
			  			"console.log(result);\n",
			  			"console.log(typeof result);\n",
			  			"const li = document.createElement('li');\n",
			  			"var htmlOut = \"<li style=\\\"margin-bottom: 20px;color: #333;font-size: 22px;text-decoration: none;\\\"><a href=\";\n",
			  			"htmlOut += result.url;\n",
			  			"htmlOut += \"\\\">\";\n",
			  			"htmlOut += result.title;\n",
			  			"htmlOut += \"</a>\";\n",
			  			"htmlOut += \"<div style=\\\"font-size: 14px;margin-top: 8px;color:grey;\\\">\";\n",
			  			"htmlOut += result.url;\n",
			  			"htmlOut += \"</div>\";\n",
			  			"htmlOut += \"<p><span style=\\\"font-size: 16px;margin: 0;\\\">\";\n",
			  			"htmlOut += result.description;\n",
			  			"htmlOut += \"</span>\";\n",
			  			"li.innerHTML = htmlOut\n",
			  			"resultsDiv.appendChild(li);\n",
			  	    "};\n",
			  	  "}\n",
			  	"}\n",
			    "});\n",
			  "}\n",
			  
			  "let timeoutId;\n",
			  "function getSuggestions() {\n",
			  "clearTimeout(timeoutId);\n",
			  "timeoutId = setTimeout(() => {\n",
			  "var input = $('#searchInput').val();\n",
			  "if(input != null && input.length>0){\n",
			  "",
			  	"$.ajax({\n",
			  	"url: '/suggestions',\n",
			  	"dataType: 'json',\n",
			  	"data: {input: input},\n",
			  	"success: function(response) {\n",
			  		"var suggestions = response.suggestions;\n",
			  		"console.log(suggestions);\n",
			  		"console.log(typeof suggestions);\n",
			  		"var htmlOut = \"\";\n",
			  		"for (var i=0; i<suggestions.length; i++)\n",
			  		"	htmlOut += \"<div style=\\\"text-align: left;font-size: 18px;padding: 8px 12px;width: auto;border-radius: 3px;\\\" class=\\\"suggestion-item\\\">\" + suggestions[i]+ \"</div>\";\n",
			  		"document.getElementById('suggestions').innerHTML = htmlOut;\n",
			  		"const suggestionElements = document.querySelectorAll(\".suggestion-item\");\n",
					   "suggestionElements.forEach(element => {\n",
					   "element.addEventListener(\"click\", function() {\n",
					   "console.log(element.textContent);\n",
					   "document.getElementById(\"searchInput\").value = element.textContent;\n",
					   "});\n",
					   "});\n",
			  	"}\n",
			   "});\n",
			   "}\n",
			  "}, 1000);\n",
			  "}\n",
			  
			  "function autofill(suggestion) {\n",
			  "document.getElementById(\"queryinput\").value = suggestion;\n",
			  "}\n",
			  
			  "function submitHandler() {\n",
			  
			  "const input = document.getElementById('searchInput').value;\n",
			  "const cleanedInput = input.replace(/\\s+/g, \" \").trim();\n",
			  "console.log('cleaned: ' + cleanedInput);\n",
			  "console.log('cleaned: ' + cleanedInput);\n",
			  "console.log('cleaned: ' + cleanedInput);\n",
			  "console.log('cleaned: ' + cleanedInput);\n",
			  "console.log('cleaned: ' + cleanedInput);\n",
			  "console.log('cleaned: ' + cleanedInput);\n",
			  "document.getElementById('searchInput').value = cleanedInput;\n",
			  "const form = document.getElementById('my-form');\n",
			  "form.submit();\n",
			  "}\n",
			  
			  "</script></head>\n",
			  "<body>\n",
			  "<div class=\"wrapper\" style=\"max-width: 450px;margin: 150px auto;\">",
			  "<center style=\"font-size:500%;\">\n",
			  "<span style=\"font-family: Arial, sans-serif;color:blue;\">5</span>",
			    "<span style=\"font-family: Arial, sans-serif;color:red;\">5</span>",
			    "<span style=\"font-family: Arial, sans-serif;color:gold;\">5</span>",
			    "<span style=\"font-family: Arial, sans-serif;color:blue;\">o</span>",
			    "<span style=\"font-family: Arial, sans-serif;color:green;\">g</span>",
			    "<span style=\"font-family: Arial, sans-serif;color:red;\">l</span>",
			    "<span style=\"font-family: Arial, sans-serif;color:gold;\">e</span>\n",
			  "</center>\n",
			  "<p>\n",
			  "<div style=\"height:20px;\"></div>\n",
			  
			  
			  "<div class=\"search-input\" style=\"position: relative;background: #efefef;width: 100%;border-radius: 5px;box-shadow: 0px 1px 5px 3px rgba(0,0,0,0.12);\">",
			  "<form style=\"background-color: transparent;\" id=\"my-form\" method=\"get\" action=\"/search\">\n",
			  "<input style=\"height: 55px;width: 100%;outline: none;border: none;border-radius: 5px;padding: 0 60px 0 20px;font-size: 18px;box-shadow: 0px 1px 5px rgba(0,0,0,0.1);\" type=\"text\" placeholder=\"Search...\" id=\"searchInput\" onkeyup=\"getSuggestions()\" size=50 name=\"query\" />\n",
			  
			  "<div id=\"submit-button\" onclick=\"submitHandler()\" value=\"search\" style=\"background-size: 70%;background-repeat: no-repeat;background-position: center center;background-image: url('https://cdn3.iconfinder.com/data/icons/essential-70/50/search-512.png');cursor: pointer;position: absolute;font-size: 20px;right: 0px;top: 0px;height: 55px;width: 55px;text-align: center;line-height: 55px;\"><i class=\"fas fa-search\"></i></div>",
			  "</form>\n",
			  "<div id=\"suggestions\"></div>",
			  "</div>",
			  "<p></div>\n" };
	public static String footer = "</body>\n</html>\n";
	public static String kvsWorker = "127.0.0.1:8001";
	public static int PAGE_SIZE = 10;

	public static String buildSuggestion(String[] q) {
		String out = "";
		for(int i = 0; i < q.length; i++) {
			out += q[i] + " ";
		}
		return out.trim();
	}

	public static HashMap<String, Double> calcTFIDF(String query, HashMap<String, Double> map, double N) {
		HashMap<String, Double> queryWeights = new HashMap<>();
		String[] querySpl = query.split(" ");
		
		//for each word, compute its tf in the query string
		for(int i = 0; i < querySpl.length - 2; i++) { //do not include last word in query w/ len - 2
			if(!queryWeights.containsKey(querySpl[i])) {
				queryWeights.put(querySpl[i], 1.0);
			} else {
				queryWeights.put(querySpl[i], queryWeights.get(querySpl[i])+1.0);
			}
		}
		
		//set to negative for sorting purposes
		for(Map.Entry<String, Double> e : map.entrySet()) {
			double idf = Math.log10(N / ((double)e.getValue()) );
			e.setValue((((double)e.getValue()) * idf)*-1);
		}
		
		return map;
	}

	public static List<String> getSuggestions(String kvsMaster, String query) throws Exception {
		String[] queryWords = query.split(" ");
			String incomplete = queryWords[queryWords.length - 1];
			KVSClient kvs = new KVSClient(kvsMaster);
			
			Iterator<Row> index = kvs.scan("index");
			
			HashMap<String, Double> suggestions = new HashMap<>();
			while (index.hasNext()) {
				Row e = index.next();
				if (e.key().startsWith(incomplete)) {
					String urls = e.get(e.key());
					suggestions.put(e.key(), (double)urls.split(",").length);
				}
			}
			
			
			
			
			//get count to get N
			double N;
			N = kvs.count("crawl");

			
			
			
			//calculate TFIDF values for each suggestion
			suggestions = calcTFIDF(query, suggestions, N);
			
			//sort the hashmap by tf
			List<Map.Entry<String, Double>> toSort = new ArrayList<>(suggestions.entrySet());
			Collections.sort(toSort, new Comparator<Map.Entry<String, Double>>() {

				@Override
				public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
					return o1.getValue().compareTo(o2.getValue());
				}});
			
			
			int i = 0;
			List<String> output = new ArrayList<>();
			//output the top 10 words by tf value
			for(Map.Entry<String, Double> x : toSort) {
				if(i < 10) {
					queryWords[queryWords.length - 1] = x.getKey();
					output.add(buildSuggestion(queryWords));
					i++;
				} else {
					break;
				}
				
			}

			System.out.println(output);
			
			return output;
	}


	public static List<String> getResults(String kvsMaster, String query, int page) throws Exception{
		int left = (page - 1) * PAGE_SIZE;
	    int right = page * PAGE_SIZE;
		System.out.println(page);
		int limit = 10;
		int offset = left;
		KVSClient kvs = new KVSClient(kvsMaster);

			
			limit = right - offset;
			double a = 0.5;
			Map<String, Map<String, Double>> tfs = new HashMap<>();
			Map<String, Double> idfs = new HashMap<>();
			Map<String, Double> max = new HashMap<>();
			Set<String> urlsSet = new HashSet<>();
			Map<String, Double> pageRanks = new HashMap<>();
			
			String[] words = URLDecoder.decode(query).split(" ");
			int i = 0;
			for(String word: words) {
				
				i += 1;
				if (!kvs.existsRow("index", word)) {
					System.out.println("DNE");
					continue;
				}
				Row r = kvs.getRow("index", word);//getRowPersist("index", word);
				for (String col: r.columns()) {
					String[] urls = r.get(col).split(",");
					for (String ul: urls) {
						String url = ul.substring(0, ul.lastIndexOf(":"));
						if (!tfs.containsKey(url)) {
							tfs.put(url, new HashMap<>());
						}
						Map<String, Double> urlMap = tfs.get(url);
						urlsSet.add(url);
						Row r2 = kvs.getRow("tf", url);//getRowPersist("tf", url);
						double value = 0.0;
						if (!(r2 == null || !r2.columns().contains(word))) {
							value = Double.parseDouble(r2.get(word));
						}
						if (urlMap.containsKey(word)) {
							urlMap.put(word + "," + i, value);
						} else {
							urlMap.put(word, value);
						}

						if (!max.containsKey(url)) {
							max.put(url, value);
						}
						if (value > max.get(url)) {
							max.put(url, value);
						}
						value = 0.0;
						r2 = kvs.getRow("df", word);//getRowPersist("df", word + "," + url);
						for (String col2: r2.columns()) {
							double val = Double.parseDouble(r2.get(col2));
							value += val;
						}
						idfs.put(word, value);
						value = 0.0;
						
						r2 = kvs.getRow("pageranks", url);//getRowPersist("pageranks", u);
						if (r2 != null)
						for (String col2: r2.columns()) {
							double val = Double.parseDouble(r2.get(col2));
							value += val;
						}
						pageRanks.put(url, value);
					}
				}
			}
			List<Map.Entry<String, Double>> urlValues = new LinkedList<>();
			Map<String, Double> map = new HashMap<>();
			
			for (String url: urlsSet) {
				double val = 0.0;
				Map<String, Double> wordVals = tfs.get(url);
				for (String word: wordVals.keySet()) {
					double tf = 0.0;
					if(wordVals.get(word) != 0) {
						tf = a + (1 - a) * wordVals.get(word) / max.get(url);
					}
					if (idfs.containsKey(word)) {
						val += tf * idfs.get(word);
					}
					
				}
				map.put(url, val + Math.sqrt(1 + pageRanks.get(url)));
			}
			
			for (Map.Entry<String, Double> entry: map.entrySet()) {
				if (urlValues.size() == 0) {
					urlValues.add(entry);
				} else {
					int k = 0;
					while (k < offset + limit) {
						if (k == urlValues.size()) {
							urlValues.add(entry);
							break;
						}
						Map.Entry<String, Double> curr = urlValues.get(k);
						if (entry.getValue() > curr.getValue()) {
							urlValues.add(k, entry);
							break;
						}
						k++;
					}
				}
			}
	    	List<String> resultUrlArray = new ArrayList<>();
			for (int j = offset; j < offset + limit && j < urlValues.size(); j++) {
				Map.Entry<String, Double> urlVal = urlValues.get(j);
				resultUrlArray.add(urlVal.getKey());
			}
			return resultUrlArray;
	}



	private static final Object lock = new Object();

	public static void main(String args[]) throws Exception {
		if (args.length < 2) {
			System.out.println("Please Specify Port and KVS master address");
			return;
		}
		String kvsMaster = args[1];
	    Server.port(Integer.parseInt(args[0]));
	    Server.get("/scroll", (req, res) -> {
			synchronized (lock) {
			String query = req.queryParams("query");
	    	String encodedString = java.net.URLEncoder.encode(query, StandardCharsets.UTF_8);
	    	int page = Integer.valueOf(req.queryParams("page"));
	    	System.out.println("search backend input: " + query);
	    	// get the url array in decreasing order of the final score
	    	// ranking range from left to right
	    	
			
			
			
			Gson gson = new Gson();
			List<String> resultUrlArray = getResults(kvsMaster, encodedString.toLowerCase(), page);
			if (resultUrlArray == null || resultUrlArray.size() == 0) {
				res.status(200, "OK");
				res.type("text/html");
				String resPage = "";
				for(String parts: header) {
					resPage += parts;
				}
				resPage += "<div class=\"error-container\">";
				resPage += "<h1 class=\"error-heading\">Result Not Found</h1>";
				resPage += "<p class=\"error-message\">We're sorry, but the results could not be found.</p>";
				resPage += "</div>";
				resPage += footer;
				return resPage;
			}
	        
			SearchResponse searchResponse = new SearchResponse(resultUrlArray);
			
	        return gson.toJson(searchResponse);
			}
	    });
	    
	    
	    Server.get("/search", (req, res) -> {
	    	String query = req.queryParams("query");
	    	String encodedString = java.net.URLEncoder.encode(query, StandardCharsets.UTF_8);
	    	int page = 1;
	    	System.out.println("search backend input: " + query);
	    	// get the url array in decreasing order of the final score
	    	// ranking range from left to right
			Gson gson = new Gson();
			List<String> resultUrlArray = getResults(kvsMaster, encodedString.toLowerCase(), page);
			if (resultUrlArray == null || resultUrlArray.size() == 0) {
				res.status(200, "OK");
				res.type("text/html");
				String resPage = "";
				for(String parts: header) {
					resPage += parts;
				}
				resPage += "<div class=\"error-container\">";
				resPage += "<h1 class=\"error-heading\">Result Not Found</h1>";
				resPage += "<p class=\"error-message\">We're sorry, but the results could not be found.</p>";
				resPage += "</div>";
				resPage += footer;
				return resPage;
			}
	    	List<Result> results = getResultsObj(resultUrlArray, query);
	    	String responsePage = getFirstPage(results, page, query);
	    	res.status(200, "OK");
	    	res.type("text/html");
	    	res.body(responsePage);
	    	return responsePage;
	    });
	    
	    Server.get("/", (req, res) -> {
	    	String homePage = getResPage(null, 0, null);
	    	res.status(200, "OK");
	    	res.type("text/html");
	    	res.body(homePage);
	    	return homePage;
	    });
	    
	    Server.get("/suggestions", (req, res) -> {
	    	String input = req.queryParams("input");
	    	System.out.println("suggestion backend input: " + input);
	    	String encodedString = java.net.URLEncoder.encode(input, StandardCharsets.UTF_8);
			List<String> suggestions = getSuggestions(kvsMaster, encodedString);
	    	// change and connect to backend
	    	/*
	    	
	    	String suggUrl = "http://" + kvsWorker + "/suggestions?query=" + encodedString;
	    	
	    	System.out.println("suggestions url: " + suggUrl);
	    	URL url = new URL(suggUrl);
	        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
	        connection.setRequestMethod("GET");
	        
	        byte[] responseBytes = connection.getInputStream().readAllBytes();
	        String responseString = new String(responseBytes, StandardCharsets.UTF_8);
	        System.out.println(responseString);
	        String[] resultSuggs = responseString.split(",");
	    	
	        
	    	// List<String> suggestions = new ArrayList<>();
	    	// suggestions.add("hello");
	    	// suggestions.add("apple");*/
	    	res.status(200, "OK");
	    	res.type("text/html");
	    	Gson gson = new Gson();
	    	Suggestions json = new Suggestions(suggestions);
	    	return gson.toJson(json);
	    });
	}
	
	// get static search result page as a string
	private static String getResPage(List<Result> results, int page, String query) {
		String res = "";
		for(String parts: header) {
			res += parts;
		}
		res += footer;
		return res;
	}
	
	private static List<Result> getResultsObj(List<String> urls, String query) {
		List<Result> results = new ArrayList<>();
		// Gson gson = new Gson();
		for(String url: urls) {
			Result res = urlToResult(url, query);
			// String json = gson.toJson(res);
			results.add(res);
		}
		return results;
	}
	
	private static String getFirstPage(List<Result> results, int page, String query) {
		String res = "";
		for(String parts: header) {
			res += parts;
		}
		if (results != null) {
			res += "<p>\n";
			res += "<div style=\"align-items: center;flex-direction: column;margin-top:-100px\">";
			// res += "<h2>Search Results</h2>";
			res += "<ul style=\"width: 80%;max-width: 800px;margin-left: auto;margin-right: auto;list-style: none;padding: 20px;\" id=\"results\">\n";
			for(Result result: results) {
				res += "<li style=\"margin-bottom: 20px;color: #333;font-size: 22px;text-decoration: none;\"><a href=\"";
				res += result.url + "\">" + result.title + "</a>\n";
				res += "<div style=\"font-size: 14px;margin-top: 8px;color:grey;\">\n";
				res += result.url;
				res += "</div>\n";
				res += "<p>\n";
				res += "<span style=\"font-size: 16px;margin: 0;\">\n";
				res += result.description;
				res += "</span>\n";
			}
			res += "</ul>\n";
			res += "</div>";
		}
		
		res += footer;
		return res;
	}
	
	
	// get list of results with given url list
	// generate json from Java Object using gson lib
	private static List<String> getResults(List<String> urls, String query) {
		List<String> results = new ArrayList<>();
		Gson gson = new Gson();
		for(String url: urls) {
			Result res = urlToResult(url, query);
			String json = gson.toJson(res);
			results.add(json);
		}
		return results;
	}
	
	// get Result Object with given url
	private static Result urlToResult(String url, String query) {
		Result res = new Result(url, null, null);
		try {
			Document doc = Jsoup.connect(url).get();
			// System.out.println("doc: " + doc);
			String title = doc.title();
			if (title == null || title.length() == 0) {
				String[] urlParts = URLParser.parseURL(url);
				title = urlParts[1] + urlParts[3];
			}
			String output = title.replaceAll("[^\\x00-\\x7F]", "");
			// System.out.println("title: " + title);
			res.setTitle(output);
			String description = getDescription(doc, query);
			// System.out.println("description: " + description);
			res.setDescription(description);
			return res;
		} catch (Exception e) {
	        e.printStackTrace();
	    }
		return res;
	}
	
	private static String getDescription(Document doc, String query) {
		String cleanQuery = query.replaceAll("\\p{Punct}", " ");
		String[] tokens = cleanQuery.split(" ");
		// try to find description from metadata
		Elements elements = doc.select("meta[name=description]");
		if (!elements.isEmpty()) {
			String input = elements.get(0).attr("content");
			String output = input.replaceAll("[^\\x00-\\x7F]", "");
			return output;
		}
		// try to find the paragraph that contains most query words
		Map<Element, Integer> count = new HashMap<>();
		for(String token: tokens) {
			Element element = doc.select("p:containsOwn(" + token + ")").first();
			if (element != null) {
				count.put(element, count.getOrDefault(element, 0) + 1);
			}
		}
		// use first paragraph as description
		if (count.isEmpty()) {
			Elements paragraphs = doc.select("p");
			if (!paragraphs.isEmpty()) {
				String input = paragraphs.get(0).text();
				String output = input.replaceAll("[^\\x00-\\x7F]", "");
				return output;
			}
		} else {
			Element max = null;
			int maxCount = -1;
			for(Element e: count.keySet()) {
				if (count.get(e) > maxCount) {
					max = e;
					maxCount = count.get(e);
				}
			}
			String input = max.text();
			String output = input.replaceAll("[^\\x00-\\x7F]", "");
			return output;
		}
		return "";
	}
	
}
