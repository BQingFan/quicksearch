package cis5550.kvs;

import static cis5550.webserver.Server.*;

public class Master extends cis5550.generic.Master {
	
	
	public static void main(String[] args) throws Exception {
		port(Integer.parseInt(args[0]));
		registerRoutes();
		get("/", (req,res) -> { 
			return "<html>\n"
					+ "<header><title>KVS Master</title></header>\n"
					+ "<body>\n"
					+ workerTable() + "\n"
					+ "</body>\n"
					+ "</html>";
			});
	}
	
}
