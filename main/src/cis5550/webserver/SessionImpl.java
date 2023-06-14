package cis5550.webserver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SessionImpl implements Session {
	
	private String id;
	private long creationTime;
	private long lastAccessedTime;
	private int maxActiveInterval;
	private boolean isValid;
	private Map<String, Object> attributes;
	
	public SessionImpl() {
		UUID uid = UUID.randomUUID();
		this.id = uid.toString();
		this.creationTime = System.currentTimeMillis();
		this.lastAccessedTime = this.creationTime;
		this.maxActiveInterval = 300;
		this.isValid = true;
		this.attributes = new HashMap<String, Object>();
	}
	
	public void setLastAccessedTime(long newTime) {
		this.lastAccessedTime = newTime;
	}

	@Override
	public String id() {
		return this.id;
	}

	@Override
	public long creationTime() {
		return this.creationTime;
	}
	
	public void updateLastAccessesTime(long newTime) {
		this.lastAccessedTime = newTime;
	}

	@Override
	public long lastAccessedTime() {
		return this.lastAccessedTime;
	}
	
	public int getMaxActiveInterval() {
		return this.maxActiveInterval;
	}
	
	@Override
	public void maxActiveInterval(int seconds) {
		this.maxActiveInterval = seconds;
	}

	@Override
	public void invalidate() {
		this.isValid = false;
	}

	@Override
	public Object attribute(String name) {
		return attributes.get(name);
	}

	@Override
	public void attribute(String name, Object value) {
		attributes.put(name, value);
	}

}
