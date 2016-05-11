package edu.cmu.qatar.cs214.hw.hw5;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.Callable;

public class MapCall implements Callable<Integer> {

	MapTask mapT;
	WorkerInfo w1;
	Partition partition;
	String time;

	/**
	 * @param mapT
	 *            map task
	 * @param w1
	 *            worker
	 * @param p
	 *            partition for the worker
	 * @param time
	 *            creation time
	 */
	public MapCall(MapTask mapT, WorkerInfo w1, Partition p, String time) {
		this.mapT = mapT;
		this.w1 = w1;
		this.partition = p;
		this.time = time;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.util.concurrent.Callable#call()
	 */
	public Integer call() throws Exception {
		// create an output stream and implement the maptask there

		Integer res = 0;
		Socket socket = null;
		try {
			socket = new Socket(w1.getHost(), w1.getPort());
			ObjectOutputStream out = new ObjectOutputStream(
					socket.getOutputStream());
			out.writeObject(new DoMapTask(mapT, w1, partition, time));

			ObjectInputStream in = new ObjectInputStream(
					socket.getInputStream());
			res = (Integer) in.readObject();
		} catch (Exception e) {

		} finally {
			try {
				if (socket != null) {
					socket.close();
				}
			} catch (IOException e) {

			}
		}
		return res;
	}
}
