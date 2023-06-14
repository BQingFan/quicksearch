package cis5550.kvs;

public class WorkerDS {
	
	private String id, ip, port;
	private long lastInvoked;
	
	public WorkerDS() {
		this.lastInvoked = System.currentTimeMillis();
		this.id = "";
		this.ip = "";
		this.port = "";
	}
	
	public WorkerDS(String id, String ip, String p) {
		this.lastInvoked = System.currentTimeMillis();
		this.id = id;
		this.ip = ip;
		this.port = p;
	}
	
	public String getID() {
		return this.id;
	}
	public String getIP() {
		return this.ip;
	}
	public String getPort() {
		return this.port;
	}
	public long getLastInvoked() {
		return this.lastInvoked;
	}
	public String getIPPort() {
		return this.ip + ":" + this.port;
	}
	
	public void setID(String x) {
		this.id = x;
	}
	public void setIP(String x) {
		this.ip = x;
	}
	public void setPort(String x) {
		this.id = x;
	}
	public void updateLastInvoked() {
		this.lastInvoked = System.currentTimeMillis();
	}
	
	public boolean isExpired() {
		return System.currentTimeMillis() - this.lastInvoked > 15000;
	}

}
