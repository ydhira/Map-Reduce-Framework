package edu.cmu.qatar.cs214.hw.hw5;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.cmu.qatar.cs214.hw.hw5.util.Log;
import edu.cmu.qatar.cs214.hw.hw5.util.WorkerStorage;

public class DoShuffle extends WorkerCommand {

	private static final long serialVersionUID = -5114768023654553676L;
	private List<WorkerInfo> mWorkers;
	private String time;
	private int i;
	private static final String TAG = "Do Shuffle";
	private WorkerInfo thisWorker;
	private static final String FILE = "/shuffle";

	public DoShuffle(List<WorkerInfo> mWorkers, String time, int i) {
		this.mWorkers = mWorkers;
		this.i = i;
		this.time = time;
		this.thisWorker = mWorkers.get(i);
	}

	@Override
	public void run() {
		Socket socket = getSocket();
		ExecutorService executor = Executors.newFixedThreadPool(Runtime
				.getRuntime().availableProcessors());
		List<RequestCallable> requestCallables = new ArrayList<RequestCallable>();

		for (WorkerInfo worker : mWorkers) {
			requestCallables.add(new RequestCallable(worker, this.i, mWorkers,
					this.time));
		}

		List<Future<HashMap<String, String>>> results;
		HashMap<String, String> shuffleResult = new HashMap<String, String>();
		try {
			results = executor.invokeAll(requestCallables);

			// init request callables

			try {
				results = executor.invokeAll(requestCallables);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}

			for (Future<HashMap<String, String>> result : results) {
				try {
					HashMap<String, String> map = result.get();
					for (Entry<String, String> pair : map.entrySet()) {
						String key = pair.getKey();
						String value = pair.getValue();
						if (shuffleResult.containsKey(key)) {
							shuffleResult.put(key, shuffleResult.get(key)
									+ value);
						} else {
							shuffleResult.put(key, value);
						}
					}
				} catch (Exception e) {
					Log.e(TAG, "execution error from shuffle request.", e);
				}

			}
		}

		catch (Exception e) {
			Log.e(TAG, "execution error from shuffle request.", e);
		}

		String file1 = WorkerStorage.getIntermediateResultsDirectory(thisWorker
				.getName());
		String path1 = file1 + FILE + time;
		File result1 = new File(path1);
		if (!result1.exists()) {
			try {
				result1.createNewFile();
			} catch (IOException e) {
				Log.e(TAG, "Error when creating new file.", e);
			}
		}

		Emitter emitter = new EmitterMap(result1);
		for (Entry<String, String> pair : shuffleResult.entrySet()) {
			try {
				emitter.emit(pair.getKey(), pair.getValue());
			} catch (IOException e) {
				Log.e(TAG, "Error.", e);
			}
		}

		ObjectOutputStream out;
		try {
			out = new ObjectOutputStream(socket.getOutputStream());
			out.writeObject(0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (executor != null) {
				executor.shutdown();
			}
		}

	}

	private class RequestCallable implements Callable<HashMap<String, String>> {

		private WorkerInfo worker;
		private int i;
		private List<WorkerInfo> mWorkers;
		private String time;

		public RequestCallable(WorkerInfo worker, int i,
				List<WorkerInfo> mWorkers2, String time) {
			this.worker = worker;
			this.i = i;
			this.mWorkers = mWorkers2;
			this.time = time;
		}

		@Override
		public HashMap<String, String> call() {
			Socket socket = null;
			HashMap<String, String> res = null;
			try {
				socket = new Socket(worker.getHost(), worker.getPort());
				ObjectOutputStream out = new ObjectOutputStream(
						socket.getOutputStream());
				out.writeObject(new ExecuteShuffle(worker, i, mWorkers, time));

				ObjectInputStream in = new ObjectInputStream(
						socket.getInputStream());

				res = (HashMap<String, String>) in.readObject();

			} catch (Exception e) {

			} finally {
				try {
					if (socket != null) {
						socket.close();
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			return res;
		}
	}
}
