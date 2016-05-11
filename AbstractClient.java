package edu.cmu.qatar.cs214.hw.hw5;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

import edu.cmu.qatar.cs214.hw.hw5.plugin.wordcount.WordCountClient;
import edu.cmu.qatar.cs214.hw.hw5.plugin.wordprefix.WordPrefixClient;
import edu.cmu.qatar.cs214.hw.hw5.util.Log;

/**
 * An abstract client class used primarily for code reuse between the
 * {@link WordCountClient} and {@link WordPrefixClient}.
 */
public abstract class AbstractClient {
	private final String mMasterHost;
	private final int mMasterPort;
	private static String TAG = "CLIENT";

	/**
	 * The {@link AbstractClient} constructor.
	 *
	 * @param masterHost
	 *            The host name of the {@link MasterServer}.
	 * @param masterPort
	 *            The port that the {@link MasterServer} is listening on.
	 */
	public AbstractClient(String masterHost, int masterPort) {
		mMasterHost = masterHost;
		mMasterPort = masterPort;
	}

	protected abstract MapTask getMapTask();

	protected abstract ReduceTask getReduceTask();

	public void execute() {
		final MapTask mapTask = getMapTask();
		final ReduceTask reduceTask = getReduceTask();

		final Tasks task = new Tasks(mapTask, reduceTask);
		Socket socket = null;
		String r = "";
		try {
			socket = new Socket(mMasterHost, mMasterPort);
			ObjectOutputStream out = new ObjectOutputStream(
					socket.getOutputStream());
			out.writeObject(task);

			ObjectInputStream in = new ObjectInputStream(
					socket.getInputStream());
			r = (String) in.readObject();

		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			try {
				if (socket != null) {
					socket.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		Log.i(TAG, "Received result ---\n" + r);

	}

	// TODO: Submit the map/reduce task to the master and wait for the task
	// to complete.

}
