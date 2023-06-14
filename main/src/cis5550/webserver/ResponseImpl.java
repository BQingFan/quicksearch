package cis5550.webserver;

import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResponseImpl implements Response {
	public Socket socket;
	public boolean isWriteCalled;
	public byte[] body;
	public int statusCode;
	public String reasonPhrase;
	public String protocol;
	public Map<String, List<String>> headers;
	public String type;
	
	public ResponseImpl(Socket client, String protocol) {
		this.isWriteCalled = false;
		this.body = null;
		this.statusCode = 200;
		this.reasonPhrase = "OK";
		this.headers = new HashMap<String, List<String>>();
		this.type = "text/html";
		this.socket = client;
		this.protocol = protocol;
	}

	@Override
	public void body(String body) {
		this.body = body.getBytes();
	}

	@Override
	public void bodyAsBytes(byte[] bodyArg) {
		this.body = bodyArg;
	}

	@Override
	public void header(String name, String value) {
		if (headers.containsKey(name)) {
			headers.get(name).add(value);
		} else {
			List<String> values = new ArrayList<String>();
			values.add(value);
			headers.put(name, values);
		}
		
	}

	@Override
	public void type(String contentType) {
		this.type = contentType;
	}

	@Override
	public void status(int statusCode, String reasonPhrase) {
		this.statusCode = statusCode;
		this.reasonPhrase = reasonPhrase;
	}

	@Override
	public void write(byte[] b) throws Exception {
		OutputStream outputStream = socket.getOutputStream();
		if (isWriteCalled == false) {
			isWriteCalled = true;
			outputStream.write((protocol + " " + statusCode + " " + reasonPhrase + "\r\n").getBytes());
			for(String name: headers.keySet()) {
				List<String> values = headers.get(name);
				for(String value: values) {
					outputStream.write((name + ": " + value + "\r\n").getBytes());
				}
			}
			outputStream.write(("Connection: close\r\n\r\n").getBytes());
		}
		outputStream.write(b);
		outputStream.flush();
	}

	@Override
	public void redirect(String url, int responseCode) {
		

	}

	@Override
	public void halt(int statusCode, String reasonPhrase) {
		

	}
	

}
