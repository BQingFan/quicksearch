package cis5550.webserver;
import java.net.SocketTimeoutException;
import javax.net.ServerSocketFactory;
import javax.net.ssl.*;
import java.security.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.LogManager;

import cis5550.tools.Logger;

public class Server {
	
	public static boolean flag = false;
	public static Server server = null;
	
	public static String rootDir;
	public static int portNumber;
	public static int securePortNumber;
	public static Set<String> acceptableMethods;
	public static Map<String, String> fileTypes;
	public static Map<Integer, String> codeStatus;
	
	public static String logFile = "serverLog.log";
	public static int NUM_WORKERS = 100;
	
	public static BlockingQueue taskQueue;
	public static Receiver receiver;
	public static Receiver secureReceiver;
	public static WorkerPool workerPool;
	public static Cleaner cleaner;
	
	public static List<String> methods;
	public static List<String> pathPatterns;
	public static List<Route> routes;
	public static Map<String, Session> sessions;
	
	private static final Logger logger = Logger.getLogger(Server.class);
	
	// server constructor
	public Server() throws Exception {
		initSetting();
		securePortNumber = -1;
		portNumber = 80;
		taskQueue = new BlockingQueue(10);
		workerPool = new WorkerPool(taskQueue, NUM_WORKERS);
		methods = new ArrayList<String>();
		pathPatterns = new ArrayList<String>();
		routes = new ArrayList<Route>();
		secureReceiver = new Receiver(taskQueue);
		receiver = new Receiver(taskQueue);
		sessions = new ConcurrentHashMap<String, Session>();
		cleaner = new Cleaner();
		rootDir = "";
	}

	// initial main function that starts the server
	// the server will be handling requests in the background while the app is free to do other things
	public void run() throws Exception {
		logFile  = rootDir + "/" + logFile;
		
		String[] args = {Integer.toString(portNumber), rootDir};
		isValidInput(args);
		setReceivers();
		workerPool.run();
	}
	
	public static class staticFiles {
		
		public static void location(String s) throws Exception {
			// check if the server is null, if so, create a server
			if (server == null) {
				server = new Server();
			}
			if (flag == false) {
				flag = true;
				rootDir = s;
				server.run();
			}
		}
		
		
	}
	
	public static void setReceivers() throws Exception {
		if (securePortNumber != -1) {
			String pwd = "secret";
			KeyStore keyStore = KeyStore.getInstance("JKS");
			keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
			KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
			keyManagerFactory.init(keyStore, pwd.toCharArray());
			SSLContext sslContext = SSLContext.getInstance("TLS");
			sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
			ServerSocketFactory factory = sslContext.getServerSocketFactory();
			ServerSocket serverSocketTLS = factory.createServerSocket(securePortNumber);
			secureReceiver.setServerSocket(serverSocketTLS);
			secureReceiver.start();
		}
		ServerSocket normalServerSocket = new ServerSocket(portNumber);
		receiver.setServerSocket(normalServerSocket);
		receiver.start();
		cleaner.start();
	}
	
	public static void get(String pathPattern, Route route) throws Exception {
		// check if the server is null, if so, create a server
		if (server == null) {
			server = new Server();
		}
		// check if the flag is false, if so, set it to true and launch run method
		if (flag == false) {
			flag = true;
			server.run();
		}
		methods.add("GET");
		pathPatterns.add(pathPattern);
		routes.add(route);
		// System.out.println("pathPattern: " + pathPattern);
	}
	
	public static void post(String pathPattern, Route route) throws Exception {
		// check if the server is null, if so, create a server
		if (server == null) {
			server = new Server();
		}
		// check if the flag is false, if so, set it to true and launch run method
		if (flag == false) {
			flag = true;
			server.run();
		}
		methods.add("POST");
		pathPatterns.add(pathPattern);
		routes.add(route);
		// System.out.println("pathPattern: " + pathPattern);
	}
	
	public static void put(String pathPattern, Route route) throws Exception {
		// check if the server is null, if so, create a server
		if (server == null) {
			server = new Server();
		}
		// check if the flag is false, if so, set it to true and launch run method
		if (flag == false) {
			flag = true;
			server.run();
		}
		methods.add("PUT");
		pathPatterns.add(pathPattern);
		routes.add(route);
		// System.out.println("pathPattern: " + pathPattern);
	}
	
	public static void port(int portNum) throws Exception {
		// check if the server is null, if so, create a server
		if (server == null) {
			server = new Server();
		}
		portNumber = portNum;
	}
	
	public static void securePort(int securePortNum) throws Exception {
		// check if the server is null, if so, create a server
		if (server == null) {
			server = new Server();
		}
		securePortNumber = securePortNum;
	}
	
	public class BlockingQueue {
		private Queue<Socket> blockingQueue;
		private int capacity;
		
		public BlockingQueue(int capacity) {
			this.capacity = capacity;
			this.blockingQueue = new LinkedList<Socket>();
		}
		public synchronized void add(Socket task) throws InterruptedException {
			while (this.blockingQueue.size() == capacity) {
				// System.out.println("blocking queue is full");
				wait();
			}
			// System.out.println("success adding a task");
			if (this.blockingQueue.size() == 0) {
				notifyAll();
			} else {
				notify();
			}
			this.blockingQueue.add(task);
		}
		
		public synchronized Socket get() throws InterruptedException {
			while (this.blockingQueue.size() == 0) {
				wait();
			}
			// System.out.println("task fetched from queue");
			if (this.blockingQueue.size() == capacity) {
				notifyAll();
			}
			return this.blockingQueue.poll();
		}
	}
	
	public class Cleaner extends Thread {
		
		public void run() {
			// System.out.println("cleaning start");
			while (true) {
				try {
					sleep(100);
					for(String name: sessions.keySet()) {
						Session session = sessions.get(name);
						boolean isExpire = checkExpire((SessionImpl) session);
						if (isExpire) {
							sessions.remove(name);
						}
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		private boolean checkExpire(SessionImpl s) {
			long current = System.currentTimeMillis();
			if (current - s.lastAccessedTime() < s.getMaxActiveInterval() * 1000) {
				s.invalidate();
				return false;
			}
			return true;
		}
	}
	
	public class Receiver extends Thread {
		private Boolean accepting = true;
		private ServerSocket serverSocket;
		private BlockingQueue taskQueue;
		
		public Receiver(BlockingQueue taskQueue) throws IOException {
			super("Thread Receiver");
			this.taskQueue = taskQueue;
		}
		
		public void run() {
			// System.out.println("Server connected");
			// System.out.println("Server: " + serverSocket.getInetAddress() + " P: " + serverSocket.getLocalPort());
			while (accepting) {
				try {
					Socket client = serverSocket.accept();
					client.setSoTimeout(3000);
					// System.out.println("Client: " + client.getLocalAddress() + " P: " + client.getPort());
					taskQueue.add(client);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		public void setServerSocket(ServerSocket serverSocket) {
			this.serverSocket = serverSocket;
		}
		
	}
	public class Worker extends Thread {
		private BlockingQueue taskQueue;
		private Socket task;
		private Boolean running;
		private int index;
		private WorkerPool pool;
		
		public Worker(BlockingQueue taskQueue, WorkerPool pool, int index) {
			super("Thread worker " + index);
			this.running = true;
			this.taskQueue = taskQueue;
			this.index = index;
			this.pool = pool;
		}
		
		public void run() {
			running = true;
			// System.out.println("in running worker");
			while(running) {
				try {
					// System.out.println("trying to get a task from queue");
					task = taskQueue.get();
					while(!task.isClosed()) {
						// System.out.println("trying to handle task");
						task.setSoTimeout(3000);
						handleTask(task);
					}
				} catch (InterruptedException e1) {
					//e1.printStackTrace();
				} catch (IOException e) {
					//e.printStackTrace();
				} finally {
					try {
						task.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		private void handleTask(Socket task) throws IOException {
			RequestHandler handler = new RequestHandler();
			handler.parseRequest(task);
			if(handler.getIsEmpty() == true) {
				task.close();
				return;
			}
			boolean isWriteCalled = handler.getIsWriteCalled();
			boolean file = handler.getFile();
			int res = handler.getRes();
			int code = handler.getCode();
			String protocol = handler.getProtocol();
			String method = handler.getMethod();
			String status = handler.getStatus();
			String body = handler.getBody();
			String type = handler.getType();
			String url = handler.getUrl();
			// System.out.println("url: " + url + " code: " +  code);
			if ((res != -1) && (code == 500) && (isWriteCalled == false)) {
				String errorString = toErrorString(protocol, code, status);
				sendResponse(errorString, task);
				task.close();
				return;
			} else if (res != -1) {
				return;
			}
			if ((code == 200) && file == true) {
				readFile(protocol, method, url, type, task);
			} else {
				String resultString = toResString(method, protocol, code, status, body, type);
				sendResponse(resultString, task);
			}
			
			
		}
		
		private String toErrorString(String protocol, int code, String status) {
			StringBuilder sb = new StringBuilder();
			sb.append(protocol + " ");
			sb.append(code + " ");
			sb.append(status);
			sb.append("\r\n");
			
			sb.append("Server: localhost\r\n");
			sb.append("Content-Length: 0\r\n");
			sb.append("\r\n");
			return sb.toString();
		}
		
		private void sendResponse(String res, Socket socket) {
			try {
				PrintStream socketOutput = new PrintStream(socket.getOutputStream());
				socketOutput.println(res);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		private void readFile(String protocol, String method, String url, String type, Socket socket) throws IOException {
			OutputStream outputStream = socket.getOutputStream();
			outputStream.write((protocol + " 200 OK\r\n").getBytes());
			outputStream.write(("Content-Type: " + type + "\r\n").getBytes());
			if ("application/octet-stream".equals(type) || "image/jpeg".equals(type)) {
				readFileBinary(socket, url, method);
			} else {
				// System.out.println("reading text file");
				readFileText(outputStream, url, method);
			}
		}
		
		private void readFileBinary(Socket socket, String url, String method) throws IOException {
			OutputStream outputStream = socket.getOutputStream();
			File file = new File(url);
			try (FileInputStream fis = new FileInputStream(file)) {
				byte[] bytes =  fis.readAllBytes();
				int length = bytes.length;
				
				// System.out.println("reading binary file" + url);
				outputStream.write(("Server: localhost\r\n").getBytes());
				outputStream.write(("Content-Length: " + length + "\r\n").getBytes());
				outputStream.write("\r\n".getBytes());
				if ("GET".equalsIgnoreCase(method)) {
					outputStream.write(bytes);
				}
			}
			outputStream.flush();
		}
		
		private void readFileText(OutputStream outputStream, String url, String method) throws FileNotFoundException {
			FileReader fr = new FileReader(url);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			StringBuilder sb = new StringBuilder();
			// System.out.println("reading text file" + url);
			try {
				while((line = br.readLine()) != null) {
					sb.append(line);
					sb.append(System.lineSeparator());
					
				}
				sb.deleteCharAt(sb.length() - 1);
				String res = sb.toString();
				byte[] bytes = res.getBytes();
				int length = 0;
				if (bytes != null) {
					length = bytes.length;
				}
				outputStream.write(("Server: localhost\r\n").getBytes());
				outputStream.write(("Content-Length: " + length + "\r\n").getBytes());
				outputStream.write("\r\n".getBytes());
				if ("GET".equalsIgnoreCase(method)) {
					outputStream.write(bytes);
				}
				outputStream.flush();
			} catch (IOException e) {
				e.printStackTrace();
			} 
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		private String toResString (String method, String protocol, int code, String status, String body, String type) {
			
			StringBuilder sb = new StringBuilder();
			int length = 0;
			if (body != null) {
				byte[] bytes = body.getBytes();
				length = bytes.length;
			}
			sb.append(protocol + " ");
			sb.append(code + " ");
			sb.append(status);
			sb.append("\r\n");
			
			sb.append("Server: localhost");
			sb.append("\r\n");
			
			sb.append("Content-Type: " + type);
			sb.append("\r\n");
			
			sb.append("Content-Length: " + length);
			sb.append("\r\n\r");
			
			
			if (!("HEAD".equalsIgnoreCase(method)) && body != null) {
				sb.append(body);
			}
			
			return sb.toString();
		}
	}
	
	
	public class WorkerPool {
		
		private int poolSize;
		private Worker[] pool;
		BlockingQueue taskQueue;
		
		public WorkerPool(BlockingQueue taskQueue, int NUM_WORKERS) {
			this.poolSize = NUM_WORKERS;
			this.taskQueue = taskQueue;
			pool = new Worker[poolSize];
			for(int i = 0; i < poolSize; i++) {
				pool[i] = new Worker(taskQueue, this, i);
			}
		}
		
		public void run() {
			for (int i = 0; i < poolSize; i++) {
				pool[i].start();
			}
		}
		
	}
	public class RequestHandler {
		private boolean isWriteCalled;
		private boolean isEmpty;
		private String protocol;
		private String method;
		private String reqUrl;
		private String url;
		private String type;
		private String body;
		private byte[] bodyArray;
		private String status;
		private boolean file;
		private int res;
		private int code;
		private int contentLength;
		private HashMap<String, String> headers;
		private Map<String,String> queryParams;
		private Map<String,String> params;
		
		public RequestHandler() {
			this.isWriteCalled = false;
			this.isEmpty = false;
			this.code = 200;
			this.file = false;
			this.bodyArray = null;
			this.contentLength = 0;
			this.headers = new HashMap<String, String>();
			this.queryParams = new HashMap<String, String>();
			this.params = new HashMap<String, String>();
		}
		
		public boolean getFile() {
			return this.file;
		}
		
		public boolean getIsEmpty() {
			return this.isEmpty;
		}
		
		public HashMap<String, String> getHeader() {
			return this.headers;
		}
		
		public String getProtocol() {
			return this.protocol;
		}
		
		public int getCode() {
			return this.code;
		}
		
		public String getMethod() {
			return this.method;
		}
		
		public String getUrl() {
			return this.url;
		}
		
		public String getStatus() {
			return this.status;
		}
		
		public String getType() {
			return this.type;
		}
		
		public String getBody() {
			return this.body;
		}
		
		public boolean getIsWriteCalled() {
			return this.isWriteCalled;
		}
		
		public boolean isFile() {
			return this.file;
		}
		
		public int getRes() {
			return this.res;
		}
		
		public void parseRequest(Socket client) throws IOException {
			// read the request
			InputStream inputStream = client.getInputStream();
			InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
			BufferedReader inputReader = new BufferedReader(inputStreamReader);
			
			String str = inputReader.readLine();
			if (str == null) {
				this.isEmpty = true;
			}
			if (str != null) {
				String[] line = str.trim().split(" ");
				
				if (line.length != 3) {
					this.code = 400;
					this.protocol = "HTTP/1.1";
					this.status = codeStatus.get(code);
					return;
				}
				
				this.method = line[0];
				this.reqUrl = line[1].trim();
				this.protocol = line[2];
				this.type = getfileType(reqUrl);
				parseHeaders(inputReader, headers);
				// setResHeaders();
				if (headers.containsKey("content-length")) {
					contentLength = Integer.parseInt(headers.get("content-length"));
					readContentByLength(inputReader, contentLength);
				}
				checkBodyQuery(this.headers, this.body);
				res = matchRoute(method, reqUrl);
				// System.out.println("matching result: " + res);
				
				if (res != -1) {
					InetSocketAddress inetSocketAddress = new InetSocketAddress(client.getInetAddress(), client.getPort());
					if (body != null) {
						bodyArray = body.getBytes();
					}
					Request request = new RequestImpl(method, reqUrl, protocol, headers, queryParams, params, inetSocketAddress, bodyArray, server);
					Response response = new ResponseImpl(client, protocol);
					try {
						Route route = routes.get(res);
						Object object = route.handle(request, response);
						checkSession((RequestImpl) request, response);
						sendResult((ResponseImpl)response, object, client);
						return;
					} catch (Exception e) {
						this.code = 500;
						this.status = codeStatus.get(500);
						this.protocol = "HTTP/1.1";
						return;
					}
				} else {
					url = Server.rootDir + reqUrl;
					// System.out.println("method: " + method + " type: " + type + " url: " + url + " protocol: " + protocol);
					requestFilter(method, type, url, protocol);
					this.status = codeStatus.get(code);
				}
			}
		}
		
		private void checkSession(RequestImpl request, Response response) {
			if (request.getIsSessionCalled() == true && request.getIsNewSession() == true) {
				String name = "Set-Cookie";
				String value = "SessionID=" + request.getSessionId();
				response.header(name, value);
			}
		}
		
		private void checkBodyQuery(Map<String, String> headers, String body) {
			if (!headers.containsKey("content-type")) {
				return;
			}
			String contentType = headers.get("content-type");
			if ("application/x-www-form-urlencoded".equalsIgnoreCase(contentType)) {
				addQuery(body);
			}
		}
		
		private void sendResult(ResponseImpl response, Object obj, Socket socket) throws IOException {
			this.isWriteCalled = response.isWriteCalled;
			if (response.isWriteCalled == true) {
				socket.close();
				return;
			}
			OutputStream outputStream = socket.getOutputStream();
			Map<String, List<String>> headers = response.headers;
			String reasonPhrase = response.reasonPhrase;
			int statusCode = response.statusCode;
			byte[] body = response.body;
			String type = response.type;
			outputStream.write((protocol + " " + statusCode + " " + reasonPhrase + "\r\n").getBytes());
			outputStream.write(("Content-Type: " + type + "\r\n").getBytes());
			for(String name: headers.keySet()) {
				List<String> values = headers.get(name);
				for(String value: values) {
					outputStream.write((name + ": " + value + "\r\n").getBytes());
				}
			}
			if (obj != null) {
				byte[] objBytes = obj.toString().getBytes();
				int length = objBytes.length;
				outputStream.write(("Content-Length: " + length + "\r\n\r\n").getBytes());
				outputStream.write(objBytes);
			} else if (obj == null && body != null) {
				int length = body.length;
				outputStream.write(("Content-Length: " + length + "\r\n\r\n").getBytes());
				outputStream.write(body);
			} else if (obj == null && body == null) {
				outputStream.write(("Content-Length: 0\r\n\r\n").getBytes());
			}
		}
		
		private int matchRoute(String method, String reqUrl) {
			
			String simplifiedPath = getQuerySimplifiedPath(reqUrl);
			// System.out.println("matching matching");
			// System.out.println("simplifiedPath start matching" + simplifiedPath + "\n");
			
			if (simplifiedPath == null) {
				return -1;
			}
			String[] reqPath = simplifiedPath.split("/");
			for(int i = 0; i < routes.size(); i++) {
				if (method.equalsIgnoreCase(methods.get(i))) {
					String[] path = pathPatterns.get(i).split("/");
					if (path.length == reqPath.length) {
						int j;
						for(j = 0; j < path.length; j++) {
							if (path[j].equals(reqPath[j])) {
								continue;
							} else if (path[j].startsWith(":")) {
								params.put(path[j].substring(1), reqPath[j]);
							} else {
								params.clear();
								break;
							}
						}
						if (j == path.length) {
							return i;
						}
						
					}
				}
			}
			return -1;
		}
		
		private String getQuerySimplifiedPath(String path) {
			if (path == null) return null;
			// System.out.println("simplifying a path");
			int index = path.indexOf("?");
			if (index == -1) {
				return path;
			}
			String simplified = path.substring(0, index);
			String queryParams = path.substring(index + 1);
			addQuery(queryParams);
			return simplified;
		}
		
		private void addQuery(String query) {
			if (!query.contains("&")) {
				if (!query.contains("=")) {
					return;
				}
				String[] pairArray = query.split("=");
				queryParams.put(java.net.URLDecoder.decode(pairArray[0], StandardCharsets.UTF_8),
						java.net.URLDecoder.decode(pairArray[1], StandardCharsets.UTF_8));
				return;
			}
			String[] queries = query.split("&");
			for(String pair: queries) {
				String[] pairArray = pair.split("=");
				queryParams.put(java.net.URLDecoder.decode(pairArray[0], StandardCharsets.UTF_8),
						java.net.URLDecoder.decode(pairArray[1], StandardCharsets.UTF_8));
			}
		}
		
		private String getfileType(String str) {
			int index;
			if ((index = str.indexOf(".")) != -1) {
				String surfix = str.substring(index).toLowerCase();
				if (fileTypes.containsKey(surfix)) {
					return fileTypes.get(surfix);
				}
			} 
			return fileTypes.get("other");
		}
		
		private void requestFilter(String method, String type, String url, String protocol) {
			String version = protocol.substring(5);
			// check 405 Not Allowed
			if ("POST".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) {
				this.code = 405;
				return;
			}
			
			// check 501 Not Implemented
			if (!("GET".equalsIgnoreCase(method)) && !("HEAD".equalsIgnoreCase(method))) {
				this.code = 501;
				return;
			}
			
			// check 505 HTTP Version Not Supported
			if (!("1.1".equals(version))) {
				this.protocol = "HTTP/1.1";
				this.code = 505;
				return;
			}
			
			// check 404 Not Found
			if (type.equals("notFile")) {
				// System.out.println("Not file: " + url);
				this.code = 404;
				return;
			}
			
			if (url.contains("..")) {
				this.code = 403;
				return;
			}
			
			File file = new File(url);
			if (!file.exists()) {
				// System.out.println("Not exist: " + url);
				this.code = 404;
				return;
			}
			
			// check 403 Forbidden
			if (!file.canRead()) {
				this.code = 403;
			}
			
			// reach here, 200 OK with readable file
			this.code = 200;
			this.file = true;
		}
		
		private void parseHeaders(BufferedReader in, Map<String, String> headers) throws IOException {
			String line = null;
			String lastHeader = null;
			while ((line = in.readLine()) != null) {
				if (line.length() == 0) {
					break;
				}
				line = line.trim();
				if (line.contains(":")) {
					int index = line.indexOf(":");
					String header = line.substring(0, index).toLowerCase();
					String content = line.substring(index + 1).trim();
					if (headers.containsKey(header)) {
						String preContent = headers.get(header);
						content = preContent + "; " + content;
						headers.put(header, content);
					} else {
						headers.put(header, content);
					}
					lastHeader = header;
				} else {
					String preContent = headers.get(lastHeader);
					String content = preContent + line;
					headers.put(lastHeader, content);
				}
			}
		}
		
		private void readContentByLength(BufferedReader in, int len) throws IOException {
			char[] readContent = new char[len];
			in.read(readContent, 0, len);
			this.body = new String(readContent);
		}
		
	}
	
	/**
	 * set log file properties
	 * @param logFile
	 */
	private static void logFileSetting(String logFile) {
		Properties properties = new Properties();
		try {
			FileInputStream configFile = new FileInputStream("/log.properties");
			properties.load(configFile);
			properties.setProperty("log.appender.file.File", logFile);
			LogManager.getLogManager().readConfiguration(configFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * check if the input for main function is valid
	 */
	private static void isValidInput(String[] args) {
		
		// check if the server is invoked with the correct number of arguments
		if (args.length != 2) {
			System.out.println("Written by Bingqing Fan");
			System.exit(-1);
		}
		
		// invoked with 2 arguments, check if it is a valid port number
		int portNum = Integer.valueOf(args[0]);
		if (portNum < 0 || portNum >= 65536) {
			System.out.println("Invalid port number. Written by Bingqing Fan");
			System.exit(-1);
		}
	}
	
	/**
	 * helper method for Server constructor
	 */
	private void initSetting() {
		
		// initial setting for file types
		fileTypes = new HashMap<String, String>();
		fileTypes.put(".jpg", "image/jpeg");
		fileTypes.put(".jpeg", "image/jpeg");
		fileTypes.put(".txt", "text/plain");
		fileTypes.put(".html", "text/html");
		fileTypes.put("other", "application/octet-stream");
		
		// initial setting for acceptable HTTP methods
		acceptableMethods = new HashSet<String>();
		acceptableMethods.add("GET");
		acceptableMethods.add("HEAD");
		
		codeStatus = new HashMap<Integer, String>();
		codeStatus.put(200, "OK");
		codeStatus.put(400, "Bad Request");
		codeStatus.put(403, "Forbidden");
		codeStatus.put(404, "Not Found");
		codeStatus.put(500, "Internal Server Error");
		codeStatus.put(501, "Not Implemented");
		codeStatus.put(505, "HTTP Version Not Supported");
	}

}
