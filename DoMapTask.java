package edu.cmu.qatar.cs214.hw.hw5;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Iterator;

import edu.cmu.qatar.cs214.hw.hw5.util.WorkerStorage;

public class DoMapTask extends WorkerCommand {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5469830913285856295L;
	private MapTask mapT;
	private WorkerInfo w1;
	private Partition partition;
	private String time;
	private static final String TAG = "DoMap";
	private static final String FILE = "/Map";

	public DoMapTask(MapTask mapT, WorkerInfo w1, Partition p, String time) {
		this.mapT = mapT;
		this.w1 = w1;
		this.partition = p;
		this.time = time;
	}

	@Override
	public void run() {
		
		Socket socket = getSocket();
		String file = WorkerStorage.getIntermediateResultsDirectory(this.w1
				.getName());
		File file2 = new File(file + FILE + time);

		for (Iterator<File> fileIterator = partition.iterator(); fileIterator
				.hasNext();) {
			File file3 = fileIterator.next();

			try {
				FileInputStream in = new FileInputStream(file3);
				mapT.execute(in, new EmitterMap(file2));

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			out.writeObject(0);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			if (socket != null) {
				socket.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
