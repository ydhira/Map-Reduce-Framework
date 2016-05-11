package edu.cmu.qatar.cs214.hw.hw5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

import edu.cmu.qatar.cs214.hw.hw5.util.WorkerStorage;

public class DoReduceTask extends WorkerCommand {

	private static final long serialVersionUID = 255172214987279578L;
	private ReduceTask reduceT;
	private WorkerInfo w;
	private String time;
	private static final String TAG = "DoReduce";

	private static final String FILE_NAME = "/shuffle_result";
	private static final String RESULT_NAME = "/final_result";

	public DoReduceTask(ReduceTask reduceT, WorkerInfo w, String time) {
		this.reduceT = reduceT;
		this.w = w;
		this.time = time;
	}

	@Override
	public void run() {
		Socket socket = getSocket();
		String file = WorkerStorage.getIntermediateResultsDirectory(this.w
				.getName());
		String shuffledFile = file + FILE_NAME + time;

		String finalFile = WorkerStorage.getFinalResultsDirectory(w.getName());
		String resultFile = finalFile + RESULT_NAME + time;

		File in = new File(shuffledFile);
		File out = new File(resultFile);
		ArrayList<String> values2 = new ArrayList<String>();
		try {
			BufferedReader in2 = new BufferedReader(new FileReader(in));
			String line;
			while ((line = in2.readLine()) != null) {
				String[] keyvalue = line.split(" ");
				String key = keyvalue[0];
				String value = keyvalue[1];

				String[] values = value.split(",");
				for (String v : values) {
					if ((!v.equals("")) || (!v.equals("\n"))) {
						values2.add(v);
					}
				}

				this.reduceT.execute(key, values2.iterator(), new EmitterMap(
						out));

			}

			ObjectOutputStream inform = new ObjectOutputStream(
					socket.getOutputStream());
			inform.writeObject(resultFile);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
