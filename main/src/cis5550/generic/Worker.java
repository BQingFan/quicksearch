package cis5550.generic;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;

public class Worker {

	public static void startPingThread(String ipPort, String port, String directory) {
		System.out.println(ipPort);
		System.out.println(directory);
		System.out.println(port);
		//look for the id file in the storage directory
		File idFile = new File(directory + "/id");
		String wrkrId = "";
		if(idFile.exists()) {
			//if it exists, read the worker's ID
			try {
				BufferedReader br = new BufferedReader(new FileReader(idFile));
				wrkrId = br.readLine();
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			//if it doesn't exist, generate a random ID & write to file
			
			//generate random id of five lower case letters
			String possibleVals = "abcdefghijklmnopqrstuvwxyz";
			for(int i = 0; i < 5; i++) {
				  wrkrId += possibleVals.charAt(new Random().nextInt(possibleVals.length()));
			}
			
			//write to file (creates if it doesn't exist?)
			try {
				BufferedWriter bw = new BufferedWriter(new FileWriter(idFile));
				bw.write(wrkrId);
				bw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		while(true) {
				try {
					Thread.sleep(5000);//this will run every five seconds
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				try {
					// http://xxx/ping?id=yyy&port=zzz
					// (where xxx, yyy, and zzz are the third command-line argument, a random string of five lower-case letters, and the first command-line argument, respectively
					URL url = new URL("http://"+ipPort+"/ping?id="+wrkrId+"&port="+port);
					url.getContent();
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
}
