package edu.cmu.qatar.cs214.hw.hw5;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.cmu.qatar.cs214.hw.hw5.util.Log;

public class MasterHelper implements Runnable {
	private Socket skt; //socket where client is accepted
	private List<WorkerInfo> mWorkers;
	private String time;
	private final ExecutorService executor;
	private static final String TAG = "MasterHelper";
	
	public MasterHelper(Socket skt, List<WorkerInfo> mWorkers){
		this.skt = skt;
		this.mWorkers = mWorkers;
		this.executor  = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}
	
	//get the map and reduce task and assign them to the workers
	public void run() {
		try {
			ObjectInputStream in = new ObjectInputStream(skt.getInputStream());
			Tasks t = (Tasks) in.readObject();
			MapTask mapT = t.getMapTask();
			ReduceTask reduceT = t.getReduceTask();
//MAP
			int i;
			while( (i = ExecuteMap (mapT)) !=0){
				if ( i==-1){
					break;
				}
			}
//SHUFFLE			
			shuffle();
//REDUCE			
			String r = ExecuteReduce(reduceT);
			try {
				if (skt!= null){
					ObjectOutputStream out = new ObjectOutputStream(skt.getOutputStream());
					out.writeObject(r);
				}
			}
			
			catch(IOException e){
				Log.e(TAG, "Error when transmitting final results back to client.", e);
			}
			
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		try {
			if (skt!= null){
				skt.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	/**
	 * @return 0,1 depending if 
	 */
	private int shuffle(){
		List<ShuffleCall> shuffleCallList= new ArrayList<ShuffleCall>();
		
		for (int i=0; i< mWorkers.size(); i++){
			shuffleCallList.add(new ShuffleCall(this.mWorkers, this.time, i));					
		}
		List<Future<Integer>> results = null;
		try{
			results = executor.invokeAll(shuffleCallList);
		}
		
		catch(Exception e){
			e.printStackTrace();
		}
		int r = 0;
		for (int i=0; i<results.size(); i++){
			ShuffleCall callable = shuffleCallList.get(i);
			Future<Integer> result = results.get(i);
			try {
				Integer res = result.get();
				r += res;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (ExecutionException e) {
				WorkerInfo down = callable.getWorker();
				mWorkers.remove(down);
				r = 1;
			}
		}
		return r;
    }
		 

	private String ExecuteReduce(ReduceTask reduceT){
		//read from the shuffled file , apply the reducetask and put the result into another file
		ArrayList<ReduceCall> rCallList = new ArrayList<ReduceCall>();
		
		for (WorkerInfo w : this.mWorkers){
			ReduceCall reduceCall = new ReduceCall(reduceT,w,time);
			rCallList.add(reduceCall);
		}
		
		List<Future<String>> result;
		String path = "";
		try {
			result = executor.invokeAll( rCallList);
			
			for (int i=0; i<result.size();i++){
				Future<String> s = result.get(i);
				path += s.get() + "\n";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return path;
	}
	
	private int ExecuteMap(MapTask mapT) {
		//get the workers and the partitions and then for each worker, do the maptask on the partition for it.
		
		//but first know which patition is assigned to which worker
		
		Date time2 = new Date();
    	String time = time2.toString();
    	
    	System.out.println("time is " + time);
		
		HashMap<Partition , List<WorkerInfo>> workerswork = new HashMap<Partition , List<WorkerInfo>>();
		List<MapCall> mapCall = new ArrayList<MapCall>(); 
		
		for (WorkerInfo w: mWorkers){
			List<Partition> ps = w.getPartitions();
			for (Partition p: ps){
				if (workerswork.containsKey(p)){
					List<WorkerInfo> ws = workerswork.get(p);
					ws.add(w);
					workerswork.put(p, ws);
				}
				
				else{
					List<WorkerInfo> ws = new ArrayList<WorkerInfo>();
					ws.add(w);
					workerswork.put(p, ws);
				}
			}
			
		}
		
		//get one worker for  partition and make the worker do the task
		for ( java.util.Map.Entry<Partition, List<WorkerInfo>> info : workerswork.entrySet()){
			Partition p = info.getKey();
			List<WorkerInfo> workers = info.getValue();
			WorkerInfo w1 = workers.get(new Random().nextInt(workers.size()));
//			WorkerInfo w1 = workers.get(0);
			MapCall mc = new MapCall(mapT,w1,p,time);
			mapCall.add(mc);
		}
		int finalr = 0;	
		try {
			List<Future<Integer>> result = executor.invokeAll( mapCall);
			for (int i=0; i< result.size(); i++){
				Future<Integer> resultfi = result.get(i);
				Integer j = resultfi.get();
				finalr += j;
			}
		
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return finalr;
		
	}

	
	
}
