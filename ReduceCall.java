package edu.cmu.qatar.cs214.hw.hw5;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Callable;

public class ReduceCall implements Callable<String> {
	ReduceTask reduceT;
	WorkerInfo w;
	String time;
	
	public ReduceCall(ReduceTask reduceT, WorkerInfo w, String time){
		this.reduceT = reduceT;
		this.w = w;
		this.time = time;
	}
	
	@Override
	public String call() throws Exception {
		
		Socket socket = null;
		String res = "";
		try{
			socket= new Socket(w.getHost(),w.getPort());
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(new DoReduceTask(reduceT, w, time));
		
			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			res = (String) in.readObject();
		}
		
		catch(Exception e){
			 try {
                 if (socket != null) {
                     socket.close();
                 }
             } catch (IOException e2) {

             }
		}
		return res;
	}

}
