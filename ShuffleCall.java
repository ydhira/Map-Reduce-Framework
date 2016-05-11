package edu.cmu.qatar.cs214.hw.hw5;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.Callable;

public class ShuffleCall implements Callable<Integer>{

	List<WorkerInfo> mWorkers;
	String time;
	int i;
	
	public ShuffleCall(List<WorkerInfo> mWorkers, String time, int i){
		this.mWorkers = mWorkers;
		this.time = time;
		this.i = i;
	}
	
	@Override
	public Integer call() throws Exception {
		WorkerInfo w = mWorkers.get(this.i);
		Socket socket = null;
		int res = 0;
		try{			
			socket = new Socket(w.getHost(), w.getPort());
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(new DoShuffle(this.mWorkers, this.time, this.i));
		
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			res = (Integer) in.readObject();
		}
		catch(Exception e){
			try {
                if (socket != null) {
                    socket.close();
                }
            } catch (IOException e1) {
            	
            }
        }
		return res;
	}

	public WorkerInfo getWorker() {
		// TODO Auto-generated method stub
		return mWorkers.get(i);
	}

	
}
