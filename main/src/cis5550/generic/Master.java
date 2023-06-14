package cis5550.generic;

import static cis5550.webserver.Server.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cis5550.kvs.WorkerDS;


public class Master {
	
	private static HashMap<String, WorkerDS> data = new HashMap<>();
	

	//returns the current list of workers as ip:port strings
	public static String getWorkers() {
		String output = "";
		if(data.size() > 0) {
			Iterator it = data.entrySet().iterator();
			while(it.hasNext()) {
				Map.Entry<String, WorkerDS> d = (Entry<String, WorkerDS>) it.next();
				WorkerDS w = d.getValue();
				if(w.isExpired()) {
					it.remove();
				} else {
					output += "\n" + d.getKey()+","+ w.getIPPort();
				}
			}
		}
		return Integer.toString(data.size()) + output;
	}
	
	//returns, as a String, the HTML table with the list of workers
	public static String workerTable() {
		
		/*
		 * <table>
		 * 	<tr><th>ID</th><th>IP</th><th>Port</th></tr>
		 * 	//for each worker:
		 * 	<tr>
		 * 		<td>entry.getKey()</td>
		 * 		<td>entry.getValue().split(":")[0]</td>
		 * 		<td>entry.getValue().split(":")[1]</td>
		 * 	</tr>
		 * </table>
		 */
		
		String x = getWorkers(); //update the list so that expired workers are removed
		
		String output = "<table border=\"1\"><tr><th>ID</th><th>IP</th><th>Port</th><th>Link</th</tr>";
		
		for(Map.Entry<String, WorkerDS> entry : data.entrySet()) {
			WorkerDS w = entry.getValue();
			output += "<tr>"
					+ "<td>" + entry.getKey() +"</td>"
					+ "<td>" + w.getIP() + "</td>"
					+ "<td>" + w.getPort() + "</td>"
					+ "<td> <a href=\"http://" + w.getIPPort() + "\">Link</a></td>"
					+ "</tr>";
		}
		
		
		return output += "</table>";
	}
	
	//creates routes for the /ping and /workers routes
	public static void registerRoutes() throws Exception {
		get("/ping", (req,res) -> { 
			data.put(req.queryParams("id"), new WorkerDS(req.queryParams("id"), req.ip(), req.queryParams("port")));
			return "OK";
		});
		get("/workers", (req,res) -> { 
			return getWorkers();
		});
	}
}
