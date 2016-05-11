package edu.cmu.qatar.cs214.hw.hw5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import edu.cmu.qatar.cs214.hw.hw5.util.Log;
import edu.cmu.qatar.cs214.hw.hw5.util.WorkerStorage;

public class ExecuteShuffle extends WorkerCommand{

	private static final long serialVersionUID = -1499795031044146391L;
	private WorkerInfo worker;
	private int i;
	private List<WorkerInfo> mWorkers;
	private String time;
	private static final String TAG = "ExecuteShuffle";
	private static final String FILE= "/shuffle2";
	

	public ExecuteShuffle(WorkerInfo worker, int i, List<WorkerInfo> mWorkers, String time){
		this.worker= worker;
		this.i = i;
		this.mWorkers = mWorkers;
		this.time = time;
		
	}
	public void run() {
		
		Socket socket = getSocket();
		
		HashMap<String, String> map = new HashMap<String, String>();
		
		// Get map result file from local storage
		String intermediateDir = 
				WorkerStorage.getIntermediateResultsDirectory(worker.getName());
		String resultDir = intermediateDir + FILE +  time;
		File file = new File(resultDir);		
		String line;
		BufferedReader in =  null;
		try {
			in = new BufferedReader(new FileReader(file));

			// read lines from map result file
			while ((line = in.readLine()) != null) {
				// parse the key/value pair
				Scanner scanner = new Scanner(line);
				String key = scanner.next();
				String value = scanner.next();
				
				// add to mapping
				if ((key.hashCode() % mWorkers.size()) == i) {
					if (map.containsKey(key)) {
						map.put(key, map.get(key) + value + ",");
					} else {
						map.put(key,  value + ",");
					}
				}
				scanner.close();
			}
		} catch (Exception e) {
			Log.e(TAG, "Error.", e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					Log.e(TAG, "I/O error.", e);
				}
			}
		}
		
		// send the mapping back to the worker (sender)
		try {
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(map);
		} catch (IOException e) {
			Log.e(TAG, "I/O error.", e);
		}
		
	}

}
