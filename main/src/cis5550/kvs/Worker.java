package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.*;
import java.net.URLDecoder;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


public class Worker extends cis5550.generic.Worker {
	
	private static ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> tables = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, RandomAccessFile> persistentTables = new ConcurrentHashMap<>();
	
	public static void occupyTables(String[] args) {
		File f = new File(args[1]);
		File[] onDisk = f.listFiles();
		
		for(int i = 0; i < onDisk.length; i++) {
			String[] pathSpl = onDisk[i].getPath().split("/");
			String[] tLog = pathSpl[pathSpl.length - 1].split("\\.");
			if(tLog.length > 1 && tLog[1].equals("table")) {
				try {
					RandomAccessFile log = new RandomAccessFile(onDisk[i], "rw");
					persistentTables.put(tLog[0], log);
					ConcurrentHashMap<String, Row> newT = new ConcurrentHashMap<>();
					
					RandomAccessFile raf = new RandomAccessFile(onDisk[i], "rw");
					Long curPos = raf.getFilePointer();
					Row r = Row.readFrom(raf);
					while(r != null) {
						//each line represents a row, but only store the pos
						Row posR = new Row(r.key());
						posR.put("pos", Long.toString(curPos));
						newT.put(r.key(), posR);
						
						curPos = raf.getFilePointer();
						r = Row.readFrom(raf);
					}
					//at the end of it, add to in-memory tables
					tables.put(tLog[0], newT);
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	
	public static void putRowPersist(String tableName, String rowName, String colVal, byte[] val) throws IOException {
		RandomAccessFile log = persistentTables.get(tableName);
		//if the row already exists in the persistent table, then update and replace value
		if(tables.get(tableName).containsKey(rowName)) {
			
			//first extract row pos in file (it should be saved in the "pos" column)
			Row r = tables.get(tableName).get(rowName);
			String pos = r.get("pos");
			
			//then read the row and put values into in-memory row 
			try {
				log.seek(Long.parseLong(pos));
				Row midR = Row.readFrom(log);
				tables.get(tableName).put(rowName, midR); //update the in-memory row to contain the actual values now (should overwrite pos)
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			r = tables.get(tableName).get(rowName); //update r pointer to row w/ values
			//now add/update the column value
			r.put(colVal, val);
			//then extract with r.toByteArray() and write value to log (in newest position available)
			byte[] logVal = r.toByteArray();
			pos = Long.toString(log.length());
			try {
				log.seek(log.length());
				log.write(logVal);
				log.write("\n".getBytes());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//once written to log, then reset row to just contain the pos column
			Row posR = new Row(rowName);
			posR.put("pos", pos);
			tables.get(tableName).put(rowName, posR);
			
		} 
		//if the row doesn't exist yet, then we need to add it
		else {
			//create row and add column, get value of row
			Row r = new Row(rowName);
			r.put(colVal, val);
			byte[] logVal = r.toByteArray();
			
			//save to log file
			
			Long pos = log.getFilePointer();
			try {
				log.write(logVal);
				log.write("\n".getBytes());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//now just store the pos column in the in-memory row
			Row posR = new Row(rowName);
			posR.put("pos", Long.toString(pos));
			tables.get(tableName).put(rowName, posR);
		}
	}
	
	public static byte[] getRowPersist(String tableName, String rowName, String colVal) throws Exception {
		RandomAccessFile log = persistentTables.get(tableName);
		Long pos = Long.parseLong(tables.get(tableName).get(rowName).get("pos"));
		log.seek(pos);
		Row r = Row.readFrom(log);
		return r.getBytes(colVal);
	}

	public static Row getRowPersist(String tableName, String rowName) throws Exception {
		RandomAccessFile log = persistentTables.get(tableName);
		Row row = tables.get(tableName).get(rowName);
		if (row == null) {
			return null;
		}
		Long pos = Long.parseLong(row.get("pos"));
		log.seek(pos);
		Row r = Row.readFrom(log);
		return r;
	}
	
	
	public static void putRow(String tableName, String rowName, String colVal, byte[] val, String filePath) throws Exception {
		if(tables.containsKey(tableName)) {
			if(tables.get(tableName).containsKey(rowName)) {
				Row r = tables.get(tableName).get(rowName);
				r.put(colVal, val);
				
				//write row to file
				File tf = new File(filePath+File.separator+tableName+".table");
				RandomAccessFile raf = new RandomAccessFile(tf, "rw");
				raf.seek(raf.length());
				raf.write(r.toByteArray());
				raf.write("\n".getBytes());

			} else { //if not, create row in the table
				Row r = new Row(rowName);
				r.put(colVal, val);
				tables.get(tableName).put(rowName, r);
				
				//write row to file
				File tf = new File(filePath+File.separator+tableName+".table");
				RandomAccessFile raf = new RandomAccessFile(tf, "rw");
				raf.seek(raf.length());
				raf.write(r.toByteArray());
				raf.write("\n".getBytes());
			}
		} else { //otherwise, create the table name with the new table entry
			ConcurrentHashMap<String, Row> newTable = new ConcurrentHashMap<>();
			Row r = new Row(rowName);
			r.put(colVal, val);
			newTable.put(rowName, r);
			tables.put(tableName, newTable);
			
			//create the .table file
			File tf = new File(filePath+File.separator+tableName+".table");
			tf.getParentFile().mkdirs();
			tf.createNewFile();
			
			//then write to it
			RandomAccessFile raf = new RandomAccessFile(tf, "rw");
			raf.write(r.toByteArray());
			raf.write("\n".getBytes());
		}
	}
	
	
	//helper functions to keep converting the in-memory persistent tables between pos value only and actual values
	public static void posToVals(String tableName) throws Exception {
		RandomAccessFile log = persistentTables.get(tableName);
		ConcurrentHashMap<String, Row> newT = new ConcurrentHashMap<>();

		log.seek(0); //reset position of log raf to start
		Row r = Row.readFrom(log);
		while(r != null) {
			//each line represents a row, so add row to newT map
			newT.put(r.key(), r);
			r = Row.readFrom(log);
		}
		log.seek(0); //reset position of log raf to start
		tables.put(tableName, newT); //and then update the in-memory table to reflect changes
	}
	public static void valsToPos(String tableName) throws Exception {
		RandomAccessFile log = persistentTables.get(tableName);
		ConcurrentHashMap<String, Row> newT = new ConcurrentHashMap<>();

		log.seek(0); //reset to start of file
		Long curPos = log.getFilePointer();
		Row r = Row.readFrom(log);
		while(r != null) {
			//each line represents a row, but only store the pos
			Row posR = new Row(r.key());
			posR.put("pos", Long.toString(curPos));
			newT.put(r.key(), posR);
			
			curPos = log.getFilePointer();
			r = Row.readFrom(log);
		}
		//at the end of it, add to in-memory tables
		tables.put(tableName, newT);
	}
	
	
	
	
	public static void main(String[] args) throws Exception {
		
		occupyTables(args);
		
		port(Integer.parseInt(args[0]));
		
		//A PUT to /data/<T>/<R>/<C> should set column C in row R of table T to the (possibly binary) data in the body of the request		
		put("/data/:tName/:tRow/:tCol", (req,res) -> {
			//if the query params for EC1 are included, only execute under correct circumstances
			if(req.queryParams("ifcolumn") != null && req.queryParams("equals") != null) {
				Row r = tables.get(req.params("tName")).get(req.params("tRow"));
				if(r != null && r.get(req.queryParams("ifcolumn")).equals(req.queryParams("equals")) ) {
					r.put(req.params("tCol"), req.bodyAsBytes());
					return "OK";
				} else {
					return "FAIL";
				}
			} 
			//if the query params are not included, then just treat like a regular put request
			else {
				if(persistentTables.containsKey(req.params("tName"))) {
					putRowPersist(req.params("tName"), req.params("tRow"), req.params("tCol"), req.bodyAsBytes());
				} else {
					putRow(req.params("tName"), req.params("tRow"), req.params("tCol"), req.bodyAsBytes(), args[1]);
				}
				return "OK";
			}
		});
		
		//a GET for /data/<T>/<R>/<C> should return the data in column C of row R in table T if the table, row, and column all exist
		get("/data/:tName/:tRow/:tCol", (req,res) -> { 
			if(tables.containsKey(req.params("tName")) && tables.get(req.params("tName")).containsKey(req.params("tRow"))) {
				byte[] output;
				if(persistentTables.containsKey(req.params("tName"))){
					output = getRowPersist(req.params("tName"), req.params("tRow"), req.params("tCol"));
				} else {
					output = tables.get(req.params("tName")).get(req.params("tRow")).getBytes(req.params("tCol"));
				}
				if(output == null) {
					res.status(404, "Not Found");
				} else {
					res.status(200, "OK");
					res.bodyAsBytes(output);
				}
				return null;
			}
			res.status(404, "Not Found");
			return null;
		});
		
		
		get("/", (req,res) -> {
			String output = "<html>"
					+ "<table border=\"1\"><tr><th>Name</th><th># of Keys</th><th>Persistent?</th></tr>";
			for(Map.Entry<String, ConcurrentHashMap<String, Row>> table : tables.entrySet()) {
				
				if(persistentTables.containsKey(table.getKey())) {
					output += "<tr>"
							+ "<td> <a href=\"/view/"+table.getKey()+"\">"+table.getKey()+"</a></td>";
					
					//get the actual # of rows from the persistent table
					RandomAccessFile log = persistentTables.get(table.getKey());
					ConcurrentHashMap<String, Row> newT = new ConcurrentHashMap<>();

					Row r = Row.readFrom(log);
					while(r != null) {
						//each line represents a row, so add row to newT map
						newT.put(r.key(), r);
						r = Row.readFrom(log);
					}
					log.seek(0); //reset position of log raf
					String size = Integer.toString(newT.size());
					
					//now newT is the correct size of the persistent table, so we can fill out that column val
					output += "<td>"+size+"</td><td>persistent</td>";
					
				} else {
					ConcurrentHashMap<String, Row> dataTable = table.getValue();
					// Each row: table name, # of keys, "persistent" if the table is persistent
					output += "<tr>"
							+ "<td> <a href=\"/view/"+table.getKey()+"\">"+table.getKey()+"</a></td>"+
							"<td>"+dataTable.size()+"</td><td></td>";
				}
						
			}
			return output + "</table></html>";
		});
		
		get("/view/:tableName", (req,res) -> {
			
			if(!tables.containsKey(req.params("tableName"))) {
				res.status(404, "Not Found");
				return null;
			}
			
			if(persistentTables.containsKey(req.params("tableName"))) {
				//fill in-memory table with actual values
				posToVals(req.params("tableName"));
			}
			
			
			//get table and then only get first 10 results?
			ConcurrentHashMap<String, Row> t = tables.get(req.params("tableName"));
			
			ArrayList<String> tS = new ArrayList<>(t.keySet());
			Collections.sort(tS);
			
			//table is now sorted by row key, so get first 10 that match w/ query params
			ArrayList<Row> displayedRows = new ArrayList<>();
			ArrayList<String> allCols = new ArrayList<>();
			
			int rows = 0;
			int i = 0;
			while(rows < 10) {
				//if we have reached end of rows, break
				if(i >= tS.size()) {
					break;
				}
				if(req.queryParams("fromRow") != null && tS.get(i).compareTo(req.queryParams("fromRow")) < 0) {
					i++;
					continue;
				} else {
					//add the row to the displayed rows list
					Row r = tables.get(req.params("tableName")).get(tS.get(i));
					displayedRows.add(r);
					//get all column values of this row and add them (if not already in the list of cols being displayed)
					Set<String> cols = r.columns();
					for(String s : cols) {
						if(!allCols.contains(s)) {
							allCols.add(s);
						}
					}
					//increase # of rows, and increase iterator to get accurate row ID
					rows++;
					i++;
				}
			}
			
			//now we have a list of up to 10 columns to be displayed on this page, with all the columns included
			//so all that's left is to make the table now
			String output = "<html><h1>"+req.params("tableName")+"</h1><table border=\"1\">";
			
			//sort the columns alphabetically; add in the row ID and row key columns
			Collections.sort(allCols);
			allCols.add(0, "Row ID");
			allCols.add(0, "Row Key");
			
			//add columns to output
			output += "<tr>";
			for(String colName : allCols) {
				output += "<th>"+colName+"</th>";
			}
			output += "</tr>";
			
			//now that we have the columns, fill out each row using the displayedRows list
			for(int j = 0; j < displayedRows.size(); j++) {
				Row r = displayedRows.get(j);
				output += "<tr><td>"+r.key()+"</td><td>"+tS.indexOf(r.key())+"</td>";
				
				//now add the values for each column
				for(int c = 2; c < allCols.size(); c++) { //start at 2 to skip row id and key cols
					if(r.get(allCols.get(c)) != null) {
						output += "<td>"+r.get(allCols.get(c))+"</td>";
					} else {
						output += "<td></td>";
					}
				}
			}
			output += "</table>";
			
			//now that the table is complete, provide "Next" link if there were more rows to display
			if(i < tS.size()) {
				String fromRow = tS.get(i);
				output += "<a href=\"/view/"+req.params("tableName")+"?fromRow="+fromRow+"\">Next</a>";
			}
			
			if(persistentTables.containsKey(req.params("tableName"))) {
				//now convert back to just storing the pos
				valsToPos(req.params("tableName"));
			}

			
			return output += "</html>";
		});
		
		put("/persist/:newTable", (req, res) -> {
			String tName = req.params("newTable");
			//if the table already exists, error
			if(tables.contains(tName) || persistentTables.containsKey(tName)) {
				res.status(403, "Forbidden");
				return "FAIL";
			} else {
				//create a new, empty persistent table if it doesn't exist, w/ mapping to log file
				tables.put(tName, new ConcurrentHashMap<String, Row>());
				
				//create a file to hold the table (the log file)
				File tf = new File(args[1]+File.separator+tName+".table");
				tf.getParentFile().mkdirs();
				tf.createNewFile();
				
				persistentTables.put(tName, new RandomAccessFile(tf, "rw"));
				
				return "OK";
			}

		});
		
		put("/rename/:oldName", (req,res) -> {

			String old = req.params("oldName");
			String nt = req.body();
			if(tables.containsKey(nt)) {
				res.status(409, "Conflict");
				return "FAIL";
			} else if (!tables.containsKey(old)){
				res.status(404, "Not Found");
				return "FAIL";
			} else {
				//rename the table in memory
				ConcurrentHashMap<String, Row> temp = tables.get(old);
				tables.remove(old);
				tables.put(nt, temp);
				
				if(persistentTables.containsKey(req.params("oldName"))) {
					//also rename the table's log file if persistent table
					File oldFile = new File(args[1]+File.separator+old+".table");
					File newFile = new File(args[1]+File.separator+nt+".table");
					oldFile.renameTo(newFile);
				}
				
				return "OK";
			}

		});
		
		put("/delete/:tName", (req,res) -> {
			String t = req.params("tName");
			
			if(tables.containsKey(t)) {
				tables.remove(t);
				if(persistentTables.containsKey(t)) {
					persistentTables.remove(t);
				}
				//delete the file
				File f = new File(args[1]+File.separator+t+".table");
				if(f.exists()) {
					f.delete();
				}
				return "OK";
			} else {
				res.status(404, "Not Found");
				return "FAIL";
			}
		});
		
		get("/count/:tName", (req,res) -> {
			String t = req.params("tName");
			
			if(persistentTables.containsKey(req.params("tName"))) {
				//fill in-memory table with actual values
				posToVals(req.params("tableName"));
			}

			if(tables.containsKey(t)) {
				res.status(200, "OK");
				String size = Integer.toString(tables.get(t).size());
				
				if(persistentTables.containsKey(req.params("tName"))) {
					//fill in-memory table back with pos values
					valsToPos(req.params("tableName"));
				}
				
				return size;
			} else {
				res.status(404, "Not Found");
				return null;
			}
		});
		
		get("/data/:tbl/:row", (req,res) -> {
			String t = req.params("tbl");
			
			if(persistentTables.containsKey(t)) {
				//fill in-memory table with actual values
				posToVals(req.params("tableName"));
			}
			
			if(tables.containsKey(t) && tables.get(t).containsKey(req.params("row"))) {
				Row r = tables.get(t).get(req.params("row"));
				res.bodyAsBytes(r.toByteArray());
				res.status(200, "OK");
				
				if(persistentTables.containsKey(t)) {
					//fill in-memory table with actual values
					valsToPos(req.params("tableName"));
				}
				
				return null;
			} else {
				res.status(404, "Not Found");
				return null;
			}
		});
		
		put("/data/:tbl", (req,res) -> {
			
			if(!tables.containsKey(req.params("tbl"))) {
				tables.put(req.params("tbl"), new ConcurrentHashMap<String, Row>());
			}
			ConcurrentHashMap<String, Row> t = tables.get(req.params("tbl"));
			
			InputStream data = new ByteArrayInputStream(req.bodyAsBytes());
			Row r = Row.readFrom(data);
			
			RandomAccessFile log = null;
			if(persistentTables.containsKey(req.params("tbl"))) {
				log = persistentTables.get(req.params("tbl"));
				log.seek(log.length()); //set to end of file
			}
			

			if(log != null) {
				while(r != null) {
					//write the value to the newest line and add/update the pos in the row in the table
					log.seek(log.length()); //set to end of file
					Long curPos = log.getFilePointer();
					log.write(r.toByteArray());
					log.write("\n".getBytes());
					Row rPos = new Row(r.key());
					rPos.put("pos", Long.toString(curPos));
					t.put(r.key(), rPos);
					r = Row.readFrom(data);
				}
			} else {
				while(r != null) {
					t.put(r.key(), r);
					r = Row.readFrom(data);
				}
			}
			
			
			return "OK";
		});
		
		get("/data/:tbl", (req,res) -> {
			if(!tables.containsKey(req.params("tbl"))) {
				res.status(404, "Not Found");
				return null;
			}
			
			if(persistentTables.containsKey(req.params("tbl"))) {
				//if it was a persistent table, fill in with actual values from .table file
				posToVals(req.params("tbl"));
			}
			
			ConcurrentHashMap<String, Row> t = tables.get(req.params("tbl"));
			byte[] lf = {(byte)0x0A};
			
			for(Map.Entry<String, Row> entry : t.entrySet()) {
				String rowKey = entry.getKey();
					if(req.queryParams("startRow") != null && rowKey.compareTo(req.queryParams("startRow")) < 0) {
						continue;
					}
					if(req.queryParams("endRowExclusive") != null && rowKey.compareTo(req.queryParams("endRowExclusive")) >= 0) {
						continue;
					}
					res.write(entry.getValue().toByteArray());
					res.write(lf);
			}
			res.write(lf);
			res.type("text/plain");
			res.status(200, "OK");
			
			if(persistentTables.containsKey(req.params("tbl"))) {
				//if it was a persistent table, go back to pos vals
				valsToPos(req.params("tbl"));
			}
			
			return null;
		});
		//public static List < String > processQuery(String query, int left, int right)

		get("/search", (req, res) -> {
			if (req.queryParams("query") == null) {
				return "";
			}
			int offset = 0;
			System.out.println("query: " + req.queryParams("query"));
			System.out.println("left: " + req.queryParams("left"));
			System.out.println("right: " + req.queryParams("right"));
			if (req.queryParams("left") != null) {
				offset = Integer.parseInt(req.queryParams("left"));
			}
			int limit = 10;
			int right = offset + limit;
			if (req.queryParams("right") != null) {
				right = Integer.parseInt(req.queryParams("right"));
			}
			limit = right - offset;
			double a = 0.5;
			Map<String, Map<String, Double>> tfs = new HashMap<>();
			Map<String, Double> idfs = new HashMap<>();
			Map<String, Double> max = new HashMap<>();
			Set<String> urlsSet = new HashSet<>();
			Map<String, Double> pageRanks = new HashMap<>();
			String query = req.queryParams("query");
			String[] words = URLDecoder.decode(query, "UTF-8").split(" ");
			int i = 0;
			for(String word: words) {
				
				i += 1;
				Row r = getRowPersist("index", word);
				if (r == null) {
					continue;
				}
				for (String col: r.columns()) {
					String[] urls = r.get(col).split(",");
					for(int j = 0; j < urls.length; j++) {
						int pos = urls[j].lastIndexOf(":");
						urls[j] = urls[j].substring(0, pos);
					}
					// System.out.println("urls for word " + word + ": ");
					for(int j = 0; j < urls.length; j++) {
						System.out.println(urls[j]);
					}
					for (String url: urls) {
						if (!tfs.containsKey(url)) {
							tfs.put(url, new HashMap<>());
						}
						Map<String, Double> urlMap = tfs.get(url);
						urlsSet.add(url);
						Row r2 = getRowPersist("tf", url);
						// System.out.println("got a row in tf");
						double value = Double.parseDouble(r2.get(word));
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
						// System.out.println("value: " + value);
						r2 = getRowPersist("df", word);
						// System.out.println("got a row in df");
						for (String col2: r2.columns()) {
							double val = Double.parseDouble(r2.get(col2));
							value += val;
						}
						idfs.put(word, value);
						value = 0.0;
						
						r2 = getRowPersist("pageranks", url);
						// System.out.println("got a row in pageranks: " + r2.toString());
						for (String col2: r2.columns()) {
							double val = Double.parseDouble(r2.get(col2));
							value += val;
						}
						pageRanks.put(url, value);
						
					}
				}
			}
			System.out.println("get all data");
			List<Map.Entry<String, Double>> urlValues = new LinkedList<>();
			Map<String, Double> map = new HashMap<>();
			
			for (String url: urlsSet) {
				double val = 0.0;
				Map<String, Double> wordVals = tfs.get(url);
				for (String word: wordVals.keySet()) {
					double tf = a + (1 - a) * wordVals.get(word) / max.get(url);
					val += tf * idfs.get(word);
				}
				map.put(url, val + pageRanks.get(url));
				// map.put(url, val);
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
			System.out.println("did ranking");
//			String bod = "[";
//			for (int j = offset; j < offset + limit && j < urlValues.size(); j++) {
//				Map.Entry<String, Double> urlVal = urlValues.get(j);
//				bod += "{\"url\":" + urlVal.getKey() + ", ";
//				bod += "\"tfidf\":" + urlVal.getValue() + ", ";
//				bod += "\"pagerank\":" + pageRanks.get(urlVal.getKey()) + ", ";
//				bod += "\"tfidfpluspagerank\":" + (urlVal.getValue() + pageRanks.get(urlVal.getKey())) + "},\n"; 
//			}
//			bod = bod.substring(0, bod.lastIndexOf(","));
//			bod += "]";
			
			String bod = "";
			for (int j = offset; j < offset + limit && j < urlValues.size(); j++) {
				Map.Entry<String, Double> urlVal = urlValues.get(j);
				if(j == offset) {
					bod += urlVal.getKey();
				} else {
					bod += ","+urlVal.getKey();
				}
				
			}
			if (bod.length() == 0) {
				res.status(404, "Not Found");
				res.type("text/plain");
				return "";
			}
			res.body(bod);
			res.status(200, "OK");
			res.type("text/plain");
			return bod;
		});
		
		//query suggestions for project
		get("/suggestions", (req, res) -> {
			String query = URLDecoder.decode(req.queryParams("query"), "UTF-8");
			if(query == null) {
				return null;
			}
			String[] queryWords = query.split(" ");
			String incomplete = queryWords[queryWords.length - 1];
			
			if(!tables.containsKey("index")) {
				res.status(404, "Not Found");
				return null;
			}
			
			if(persistentTables.containsKey("index")) {
				posToVals("index");
			}
			
			ConcurrentHashMap<String, Row> index = tables.get("index");
			HashMap<String, Double> suggestions = new HashMap<>();
			
			for(Entry<String, Row> e : index.entrySet()) {
				//row key is the word
				if(e.getKey().startsWith(incomplete)) {
					String urls = e.getValue().get(e.getKey());
					suggestions.put(e.getKey(), (double)urls.split(",").length);
				}
			}
			
			
			
			//get count to get N
			double N;
			if(persistentTables.containsKey("crawl")) {
				//fill in-memory table with actual values
				posToVals("crawl");
			}

			if(tables.containsKey("crawl")) {
				res.status(200, "OK");
				N = tables.get("crawl").size();
				
				if(persistentTables.containsKey("crawl")) {
					//fill in-memory table back with pos values
					valsToPos("crawl");
				}
			} else {
				res.status(404, "Not Found");
				return null;
			}
			
			
			//calculate TFIDF values for each suggestion
			suggestions = calcTFIDF(query, suggestions, N);
			
			//sort the hashmap by tf
			List<Entry<String, Double>> toSort = new ArrayList<Entry<String, Double>>(suggestions.entrySet());
			Collections.sort(toSort, new Comparator<Entry<String, Double>>() {

				@Override
				public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
					return o1.getValue().compareTo(o2.getValue());
				}});
			
			String output = "";
			int i = 0;
			
			//output the top 10 words by tf value
			for(Entry<String, Double> x : toSort) {
				if(i < 10) {
					queryWords[queryWords.length - 1] = x.getKey();
					if(i == 0) {
						output += buildSuggestion(queryWords);
					} else {
						output += ","+buildSuggestion(queryWords);
					}

					i++;
				} else {
					break;
				}
				
			}

			System.out.println(output);
			
			if(persistentTables.containsKey("index")) {
				//if it was a persistent table, go back to pos vals
				valsToPos("index");
			}
			return output;
		});
		startPingThread(args[2], args[0], args[1]);
		
	}
	
	//helper functions for /suggestions route
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
		for(Entry<String, Double> e : map.entrySet()) {
			double idf = Math.log10(N / ((double)e.getValue()) );
			e.setValue((((double)e.getValue()) * idf)*-1);
		}
		
		return map;
	}
	
}
