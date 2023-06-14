package cis5550.webserver;
import java.util.*;
import java.net.*;
import java.nio.charset.*;

// Provided as part of the framework code

class RequestImpl implements Request {
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String,String> headers;
  Map<String,String> queryParams;
  Map<String,String> params;
  byte bodyRaw[];
  Server server;
  Session session;
  boolean isNewSession;
  boolean isSessionCalled;

  RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
    session = null;
    isNewSession = false;
    isSessionCalled = false;
    
  }
  public String getSessionId() {
	  return session.id();
  }

  public boolean getIsSessionCalled() {
	  return isSessionCalled;
  }
  public boolean getIsNewSession() {
	  return isNewSession;
  }
  public String requestMethod() {
  	return method;
  }
  public void setParams(Map<String,String> paramsArg) {
    params = paramsArg;
  }
  public int port() {
  	return remoteAddr.getPort();
  }
  public String url() {
  	return url;
  }
  public String protocol() {
  	return protocol;
  }
  public String contentType() {
  	return headers.get("content-type");
  }
  public String ip() {
  	return remoteAddr.getAddress().getHostAddress();
  }
  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }
  public byte[] bodyAsBytes() {
  	return bodyRaw;
  }
  public int contentLength() {
  	return bodyRaw.length;
  }
  public String headers(String name) {
  	return headers.get(name.toLowerCase());
  }
  public Set<String> headers() {
  	return headers.keySet();
  }
  public String queryParams(String param) {
  	return queryParams.get(param);
  }
  public Set<String> queryParams() {
  	return queryParams.keySet();
  }
  public String params(String param) {
    return params.get(param);
  }
  public Map<String,String> params() {
    return params;
  }
  
  @Override
  public Session session() {
	  
	boolean isFound = false;
	  
	// session is called, session should be found or created
	isSessionCalled = true;
	
	if (headers.containsKey("cookie")) {
		String[] cookies = headers.get("cookie").split(";");
		Map<String, String> cookiesMap = new HashMap<String, String>();
		for(String pair: cookies) {
			String[] cookiePair = pair.trim().split("=");
			if (cookiePair.length != 2) {
				continue;
			}
			cookiesMap.put(cookiePair[0], cookiePair[1]);
		}
		if (cookiesMap.containsKey("SessionID")) {
			String sessionID = cookiesMap.get("SessionID");
			if (Server.sessions.containsKey(sessionID)) {
				session = Server.sessions.get(sessionID);
				boolean isExpired = checkExpired((SessionImpl)session);
				if (isExpired == true) {
					isFound = false;
				} else {
					isFound = true;
				}
			}
		}
	}
	if (isFound == false) {
		session = new SessionImpl();
		String sessionID = session.id();
		Server.sessions.put(sessionID, session);
		isNewSession = true;
	}
	return session;
  }

  private boolean checkExpired(SessionImpl s) {
	  long current = System.currentTimeMillis();
	  if (s.lastAccessedTime() + s.getMaxActiveInterval() * 1000 < current) {
		  s.invalidate();
		  return true;
	  }
	  s.setLastAccessedTime(current);
	  return false;
  }

}
