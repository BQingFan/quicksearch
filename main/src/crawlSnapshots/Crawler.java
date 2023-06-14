package crawlSnapshots;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileWriter;    // msn
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class Crawler {
	private static boolean isRoot = true;
	private static final String USER_AGENT = "cis5550-crawler";
	public static String[] EXTENSION_VALUES = new String[] {"jpg", "jpeg", "gif", "png", "txt"};
	public static Set<String> INVALID_EXTENSIONS = new HashSet<>(Arrays.asList(EXTENSION_VALUES));
	public static String[] REDIRECT_VALUES = new String[] {"301", "302", "303", "307", "308"};
	public static Set<String> REDIRECT_CODES = new HashSet<>(Arrays.asList(REDIRECT_VALUES));
	public static String[] EXCESS_ELEMENTS = new String[] {"style", "script", "footer"};
	
	public static String removeExcessElements(String html){

		System.out.println("remove excess elements called");


		String result = new String(html);

		//String result = "";

		for (String tag : EXCESS_ELEMENTS) {

			//System.out.println("remove " + tag);

			String regex = new String("(?m)(?s)<" + tag + "[^>]*>(.*?)</" + tag + ">");

			//System.out.println("regex = " + regex);
			//String regex = new String("<footer[^>]*>(.*?)</footer>");
			//Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(result);

			System.out.println("new group match count: " + matcher.groupCount());

			//result = matcher.replaceAll("replaced_" + tag + "_element");
			result =  matcher.replaceAll("");

			//logCrawler(result);
			//<script[^>]*>(.*?)</script>

			/*
        while (matcher.find()) {
            // Debug all that was found
            System.out.println("found: " + matcher.group(1));
        }
			 */
		}

		return result;

	}

	public static void logCrawler(String logText) {  // msn

		Path path = Paths.get("/logCrawler.txt");		

		long size;
		try {
			if (Files.exists(path)) {
				size = Files.size(path);
				
				System.out.println("log size: " + size);

				if (size > 10000) {
					// dump the log file to conserve space (new one will be created below)
					Files.delete(path);
				}	
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			FileWriter fw = new FileWriter("logCrawler.txt", true);
			fw.write("\r\n" + Long.toString(System.currentTimeMillis()) +  " - " + logText);
			fw.close();
		} catch (IOException e) {
			System.out.println("A log error occurred.");
			e.printStackTrace();
		}
	}
	
	public static List<String> extractUrls(String htmlContent, String rootUrl) {
		//logCrawler("extractUrls: " + rootUrl);   // msn
	    List<String> urls = new ArrayList<>();
	    
	    Pattern pattern = Pattern.compile("<a\\s+[^>]*href\\s*=\\s*\"([^\"]*)\"[^>]*>", Pattern.CASE_INSENSITIVE);
	    Matcher matcher = pattern.matcher(htmlContent);

	    while (matcher.find()) {
	        String url = matcher.group(1);
	        String nomalized = normalizeUrls(rootUrl, url);
	        if (nomalized != null) {
	        	urls.add(nomalized);
	        }
	    }
	    return urls;
	}
	
	public static String normalizeUrls(String rootUrl, String url) {
		//logCrawler("normalizeUrls: " + "rootUrl - " +  rootUrl + "; url - " + url);  // msn
        // remove the part after #, ignore fragment-only URL
        String[] urlParts = URLParser.parseURL(url);
        
        for (String urlPart : urlParts) {
        	if (urlPart != null) {
        		// logCrawler("urlPart: " + urlPart);
        	}
        	else {
        		// logCrawler("urlPart: + *null*");
        	}
        }
        
        if (urlParts[3].startsWith("#")) {
            return null;
        }
        int index = urlParts[3].lastIndexOf("#");
        if (index != -1) {
            urlParts[3] = urlParts[3].substring(0, index);
            if (url.length() == 0) {
                return null;
            }
        }
        // parse the URL
        String[] rootUrlParts = URLParser.parseURL(rootUrl);
        
        for (String rootUrlPart : rootUrlParts) {
        	if (rootUrlPart != null) {
        		// logCrawler("rootUrlPart: " + rootUrlPart);
        	}
        	else {
        		// logCrawler("rootUrlPart: **null**");
        	}
        }       
        
        
        // filter invalid URL
        if (urlParts[0] != null) {
            if (!urlParts[0].equalsIgnoreCase("http") && !urlParts[0].equalsIgnoreCase("https")) {
                return null;
            }
        }
        if (urlParts[0] == null || (urlParts[0] != null && urlParts[0].isEmpty())) {
            urlParts[0] = rootUrlParts[0];
        }
        int extensionIndex = urlParts[3].lastIndexOf(".");
        if (extensionIndex != -1) {
            String extension = urlParts[3].substring(extensionIndex + 1).toLowerCase();
            // System.out.println(extension);
            if (INVALID_EXTENSIONS.contains(extension)) {
                return null;
            }
        }
        // absolute link
        if (!urlParts[3].startsWith("/")) {
        	// logCrawler("absolute link - url starts with slash");
            // check relative link
            Stack<String> stack = new Stack<>();
            String[] rootPath = rootUrlParts[3].split("/");
            for(int i = 0; i < rootPath.length - 1; i++) {
                if (rootPath[i].length() == 0) {
                    continue;
                }
                stack.push(rootPath[i]);
            }
            String[] path = urlParts[3].split("/");
            for(int i = 0; i < path.length; i++) {
                if (path[i].length() == 0) {
                    continue;
                }
                if (!stack.isEmpty() && path[i].equals("..")) {
                    stack.pop();
                } else if (!path[i].equals("..")) {
                    stack.push(path[i]);
                }
            }
            StringBuilder cleanPath = new StringBuilder();
            for(int i = 0; i < stack.size(); i++) {
                cleanPath.append("/");
                cleanPath.append(stack.get(i));
            }
            urlParts[3] = cleanPath.toString();
        }

        // check host part
        if(urlParts[1] == null) {
        	// logCrawler("check host part: urlParts[1] == null");
            urlParts[0] = rootUrlParts[0];
            urlParts[1] = rootUrlParts[1];
            urlParts[2] = rootUrlParts[2];
        } else {
        	// logCrawler("check host part: urlParts[1] != null");
            if (urlParts[2] == null) {
                if (urlParts[0].equalsIgnoreCase("http")) {
                    urlParts[2] = "80";
                } else if (urlParts[0].equalsIgnoreCase("https")) {
                    urlParts[2] = "443";
                } else if (urlParts[0] == null) {
                    urlParts[0] = "http";
                    urlParts[2] = "80";
                }
            }
        }
        String res = urlParts[0] + "://" + urlParts[1] + ":" + urlParts[2] + urlParts[3];
        // logCrawler("normalized: " + res);   // msn
        if (res.contains("null")) {
        	// this sometimes happen, but it hasn't been reproducible   // msn
        	return null;
        }
        return res;
    }
	public static String nomalizeRoot(String rootUrl) {
		//logCrawler("normalizeRoot: " + rootUrl);                    // msn
        String[] rootUrlParts = URLParser.parseURL(rootUrl);
        // remove parts after #
        int rootIndex = rootUrlParts[3].indexOf("#");
        if (rootIndex != -1) {
            rootUrlParts[3] = rootUrlParts[3].substring(0, rootIndex);
        }
        // add port number
        if (rootUrlParts[2] == null) {
            if (rootUrlParts[0] == null) {
                rootUrlParts[0] = "http";
            }
            if (rootUrlParts[0].equalsIgnoreCase("http")) {
                rootUrlParts[2] = "80";
            } else if (rootUrlParts[0].equalsIgnoreCase("https")) {
                rootUrlParts[2] = "443";
            }
        }
        String res = rootUrlParts[0] + "://" + rootUrlParts[1] + ":" + rootUrlParts[2] + rootUrlParts[3];
        return res;
    }
	
	public static boolean isRateLimited(KVSClient kvs, String url) throws FileNotFoundException, IOException {
		//logCrawler("isRateLimited: " + url);                    // msn
		String hostName = URLParser.parseURL(url)[1];
		if (kvs.existsRow("hosts", hostName)) {
			byte[] lastAccess = kvs.get("hosts", hostName, "lastAccess");
			if (lastAccess == null) {
				return false;
			}
			String lastAccessStr = new String(lastAccess);
			Long lastAccessLong = Long.valueOf(lastAccessStr);
			Float crawDelay = getCrawDelay(kvs, url);
			if (System.currentTimeMillis() - lastAccessLong < 1000 * crawDelay) {
				return true;
			}
		}
		return false;
	}
	
	public static void updateTimestamp(KVSClient kvs, String url) throws IOException {
		String hostName = URLParser.parseURL(url)[1];
		String time = Long.toString(System.currentTimeMillis());
		kvs.put("hosts", hostName, "lastAccess", time);
	}
	
	public static Float getCrawDelay(KVSClient kvs, String url) throws IOException {
		//logCrawler("getCrawlDelay: " + url);                    // msn
		String[] urlParts = URLParser.parseURL(url);
		String rowKey = urlParts[1];
		byte[] crawDelay = kvs.get("hosts", rowKey, "crawDelay");
		Float res = (float) 1.0;
		if (crawDelay == null) {
			String robotContent = getRobotsTxtContent(kvs, url);
			String currDelay = "1.0";
			if (robotContent != null) {
				String[] lines = robotContent.split("\\r?\\n");
				// check for User-agent: cis5550-crawler
				for(String line: lines) {
					if (line.startsWith("Crawl-delay:")) {
						currDelay = line.substring("Crawl-delay:".length()).trim();
						
					}
				}
			}
			crawDelay = currDelay.getBytes();
			kvs.put("hosts", rowKey, "crawDelay", crawDelay);
		}
		String delay = new String(crawDelay);
		if (delay != null && delay.length() > 0) {
			res = Float.valueOf(delay);
		}
		return res;
	}
	
	public static boolean isRobotAllowed(KVSClient kvs, String url) throws IOException {
		//logCrawler("isRobotAllowed: " + url);                    // msn
		String robotContent = getRobotsTxtContent(kvs, url);
		String path = URLParser.parseURL(url)[3];
		if (robotContent != null) {
			String[] lines = robotContent.split("\\r?\\n");
			String userAgent = null;
			// check for User-agent: cis5550-crawler
			for(String line: lines) {
				if (line.startsWith("User-agent:")) {
					String curr = line.substring("User-agent:".length()).trim();
					if (curr.equals(USER_AGENT)) {
						userAgent = curr;
					} else {
						userAgent = null;
					}
				} else if(line.startsWith("Disallow:") && userAgent != null) {
					String disallowedUrl = line.substring("Disallow:".length()).trim();
					if (path.startsWith(disallowedUrl)) {
						return false;
					}
				} else if(line.startsWith("Allow:") && userAgent != null) {
					String allowedUrl = line.substring("Allow:".length()).trim();
					if (path.startsWith(allowedUrl)) {
						return true;
					}
				}
			}
			// check for User-agent: *
			for(String line: lines) {
				if (line.startsWith("User-agent:")) {
					String curr = line.substring("User-agent:".length()).trim();
					if (curr.equals("*")) {
						userAgent = curr;
					} else {
						userAgent = null;
					}
				} else if(line.startsWith("Disallow:") && userAgent != null) {
					String disallowedUrl = line.substring("Disallow:".length()).trim();
					if (path.startsWith(disallowedUrl)) {
						return false;
					}
				} else if(line.startsWith("Allow:") && userAgent != null) {
					String allowedUrl = line.substring("Allow:".length()).trim();
					if (path.startsWith(allowedUrl)) {
						return true;
					}
				}
			}
		}
		// no rules matched
		return true;
	}
	
	public static String getRobotsTxtContent(KVSClient kvs, String url) throws IOException {
		//logCrawler("getRobotsTxtContent: " + url);                    // msn
		String[] urlParts = URLParser.parseURL(url);
		String rowKey = urlParts[1];
		byte[] robotsTxtContent = kvs.get("hosts", rowKey, "robots");
		if (robotsTxtContent != null) {
			new String(robotsTxtContent, StandardCharsets.UTF_8);
		}
		urlParts[3] = "/robots.txt";
		String robotsUrl = urlParts[0] + "://" + urlParts[1] + ":" + urlParts[2] + urlParts[3];
		// file does not exsist in table, make get request
		URL robotsLink;
		try {
			robotsLink = new URL(robotsUrl);
		} catch (MalformedURLException e) {
	        return null;
	    }
		
		
		int responseCode;     // msn
		HttpURLConnection connection;
		
		try {             // msn
		connection = (HttpURLConnection) robotsLink.openConnection();
		connection.setRequestMethod("GET");
		connection.setRequestProperty("User-Agent", "cis5550-crawler");
		connection.setConnectTimeout(5000);  // msn
		connection.setReadTimeout(5000); // msn
		
	     responseCode = connection.getResponseCode();
		
		} catch (java.net.SocketTimeoutException e) {
				// logCrawler("getRobotsTxtContent - SocketTimeoutException");
			return null;

			} catch (java.io.IOException e) {
				// logCrawler("getRobotsTxtContent - SocketTimeoutException");
              return null;
			}		
		
		if (responseCode == 200) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            reader.close();
            robotsTxtContent = sb.toString().getBytes();
            kvs.put("hosts", rowKey, "robots", robotsTxtContent);
            return new String(robotsTxtContent, StandardCharsets.UTF_8);
		}
		kvs.put("hosts", rowKey, "robots", "null");
		return null;
	}
	
	public static void run(FlameContext ctx, String[] urls) throws Exception {
		
		// check if the seed url is missing
		if(urls.length == 0) {
			ctx.output("Missing seed url");
		} else {
			ctx.output("OK");
		}
		
		
		List<String> urlLists = new ArrayList<>();
		for (int i=0; i<urls.length; i++) {
			String seedUrl = urls[i];
			//seedUrl = normalizeUrl(seedUrl.trim(), "");
			urlLists.add(seedUrl);
			System.out.println("seedUrl= " + seedUrl);
		}
		FlameRDD urlQueue = ctx.parallelize(urlLists);
		
		
		/*
		// create the initial FlameRDD
		List<String> urlLists = new ArrayList<>();
		urlLists.add(urls[0]);
		FlameRDD urlQueue = ctx.parallelize(urlLists);
		*/
		
		Integer initialRowCount = ctx.getKVS().count("crawl");
		
		System.out.println("initial crawl table row count: " + initialRowCount);
		
		if (ctx.getKVS().count("crawl") == 0) {
			ctx.getKVS().persist("crawl"); // msn
		}
		
		while(urlQueue.count() > 0) {
			
			System.out.println("urlQueue.count = " + urlQueue.count() + "row count: " + ctx.getKVS().count("crawl"));
			
			//if (ctx.getKVS().count("crawl") > 20) {
				//return; // crawler is done
			//}
			
			urlQueue = urlQueue.flatMap((String value) -> {
				//System.out.println("row count in flat map: " + ctx.getKVS().count("crawl"));
				
				if (ctx.getKVS().count("crawl") > initialRowCount + 5000) {
					
					return new ArrayList<>();  // crawler is done
				}
				
				
				String rootUrl = value;
				if (isRoot) {
					rootUrl = nomalizeRoot(value);
					isRoot = false;
				}
				System.out.println("flat map - rootUrl: " + rootUrl);
				// ****** logCrawler("flat map - rootUrl: " + rootUrl);                    // msn
				// check if the URL is visited
				
				StringBuilder input = new StringBuilder();
				 
		        // append the rootUrl string into StringBuilder input
		        input.append(rootUrl);
		 
		        // reverse StringBuilder input (rootUrl) to avoid the start of the key based on http... for all urls
				String rowKey = Hasher.hash(input.reverse().toString());
				
				KVSClient kvs = ctx.getKVS();
				if (kvs.existsRow("crawl", rowKey)) {
					//System.out.println("this row exists already");
					return new ArrayList<>();
				}
				
				// https://www.washingtonpost.com:443/wp-srv/onpolitics/watergate/haldeman.html
				//java.lang.NullPointerException: Cannot invoke "java.lang.Iterable.iterator()" because the return value of "cis5550.flame.FlameRDD$StringToIterable.op(String)" is null
				
				// check /robots.txt
				boolean isAllowed = isRobotAllowed(kvs, value);
				if (isAllowed == false) {
					// System.out.println("msn - isAllowed=false for url:" + rootUrl);
					return new ArrayList<>();
				}
				// check if rate limited
				boolean isRateLimited = isRateLimited(kvs, value);
				if (isRateLimited == true) {
					// System.out.println("msn - isRateLimited=false for url:" + rootUrl);
					return new ArrayList<String>(Arrays.asList(new String[] {value}));
				}
				
				URL url = new URL(rootUrl);
				// make a HEAD request
				updateTimestamp(kvs, rootUrl);
				
				String headResponseCode = "";
				String length = "";
				String contentType = ""; 
				String redirectUrl = "";
				
				
				try {
				HttpURLConnection headConnection = (HttpURLConnection) url.openConnection();
				headConnection.setRequestMethod("HEAD");
				headConnection.setRequestProperty("User-Agent", "cis5550-crawler");
				headConnection.setInstanceFollowRedirects(false);			
				headConnection.setConnectTimeout(5000);  // msn
				headConnection.setReadTimeout(5000); // msn
				
				headResponseCode = String.valueOf(headConnection.getResponseCode());
				length = headConnection.getHeaderField("Content-Length");
				contentType = headConnection.getHeaderField("Content-Type");
				
				if (REDIRECT_CODES.contains(headResponseCode)) {
					redirectUrl = headConnection.getHeaderField("Location");
				}
				
			} catch (java.net.SocketTimeoutException e) {
				// logCrawler("run -> getHeadResponseCode - SocketTimeoutException");
				return new ArrayList<>();

			} catch (java.io.IOException e) {
				// logCrawler("run -> getHeadResponseCode - SocketTimeoutException");
				return new ArrayList<>();
			}		
							
				
				// add new row
				kvs.put("crawl", rowKey, "url", rootUrl);
				kvs.put("crawl", rowKey, "responseCode", headResponseCode);
				if (length != null) {
					kvs.put("crawl", rowKey, "length", length);
				}
				if (contentType != null) {
					kvs.put("crawl", rowKey, "contentType", contentType);
				}
				// for redirect response
				if (REDIRECT_CODES.contains(headResponseCode)) {
					//String redirectUrl = headConnection.getHeaderField("Location");
					String[] redirectUrlParts = URLParser.parseURL(redirectUrl);
					String[] rootUrlParts = URLParser.parseURL(rootUrl);
					int redirectIndex = redirectUrlParts[3].indexOf("#");
			        if (redirectIndex != -1) {
			        	redirectUrlParts[3] = redirectUrlParts[3].substring(0, redirectIndex);
			        }
			        // add port number
			        if (redirectUrlParts[2] == null) {
			            if (redirectUrlParts[0] == null) {
			            	redirectUrlParts[0] = "http";
			            }
			            if (redirectUrlParts[0].equalsIgnoreCase("http")) {
			            	redirectUrlParts[2] = "80";
			            } else if (redirectUrlParts[0].equalsIgnoreCase("https")) {
			            	redirectUrlParts[2] = "443";
			            }
			        }
					if (redirectUrlParts[1] == null) {
						redirectUrlParts[1] = rootUrlParts[1];
					}
					redirectUrl = redirectUrlParts[0] + "://" + redirectUrlParts[1] + ":" + redirectUrlParts[2] + redirectUrlParts[3];
					System.out.println("redirect to: " + redirectUrl);
					return new ArrayList<String>(Arrays.asList(redirectUrl));
				}
				
				if (headResponseCode.equals("200")) {
					// make a GET request
					updateTimestamp(kvs, value);
					
					HttpURLConnection connection;
					String responseCode = "";
					byte[] page;
					
					try {					
					connection = (HttpURLConnection) url.openConnection();
					connection.setRequestMethod("GET");
					connection.setRequestProperty("User-Agent", "cis5550-crawler");
					
					responseCode = String.valueOf(connection.getResponseCode());
					kvs.put("crawl", rowKey, "responseCode", responseCode);
					page = connection.getInputStream().readAllBytes();
					} catch (java.net.SocketTimeoutException e) {
						// logCrawler("run -> getResponseeCode " + responseCode + "- SocketTimeoutException");
						return new ArrayList<>();
					} catch (java.io.IOException e) {
						// logCrawler("run -> getResponseCode " + responseCode + " - IOException");
						return new ArrayList<>();
					}						
					
					//System.out.println(" check if text/html");
					
					if (contentType.substring(0,9).equals("text/html")) {     // msn
						// read the response
						//byte[] page = connection.getInputStream().readAllBytes();
						// System.out.println("msn - page length (200 response): " + page.length);
						
						// remove script elements
						
			            String htmlContent = new String(page);
			            
			            //System.out.println(" call removeExcessElements");
			            
			            htmlContent = removeExcessElements(htmlContent);
			            
			            // limit to English language   -- msn
			    	    String lang = "lang=\"en";
			    	    if (!(htmlContent.contains(lang))) {
			    	    	return new ArrayList<>();
			    	    }
			    	    
			            kvs.put("crawl", rowKey, "page", htmlContent);
	                    // extract urls
			      
	                    List<String> newUrls = extractUrls(htmlContent, rootUrl);
	                    return newUrls;
					}
					
				}
				return new ArrayList<>();
			});
			Thread.sleep(200);
		}
	}

}
