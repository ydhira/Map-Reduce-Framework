package edu.cmu.qatar.cs214.hw.hw5;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.cmu.qatar.cs214.hw.hw5.util.StaffUtils;

/**
 * This class represents the "master server" in the distributed map/reduce
 * framework. The {@link MasterServer} is in charge of managing the entire
 * map/reduce computation from beginning to end. The {@link MasterServer}
 * listens for incoming client connections on a distinct host/port address, and
 * is passed an array of {@link WorkerInfo} objects when it is first initialized
 * that provides it with necessary information about each of the available
 * workers in the system (i.e. each worker's name, host address, port number,
 * and the set of {@link Partition}s it stores). A single map/reduce computation
 * managed by the {@link MasterServer} will typically behave as follows:
 *
 * <ol>
 * <li>Wait for the client to submit a map/reduce task.</li>
 * <li>Distribute the {@link MapTask} across a set of "map-workers" and wait for
 * all map-workers to complete.</li>
 * <li>Distribute the {@link ReduceTask} across a set of "reduce-workers" and
 * wait for all reduce-workers to complete.</li>
 * <li>Write the locations of the final results files back to the client.</li>
 * </ol>
 */
public class MasterServer extends Thread {
	private final int mPort;
    private final List<WorkerInfo> mWorkers;
    private ArrayList<ServerSocket> sktList = new ArrayList<ServerSocket>();
    //keeps track of all the partiotions given to workers.
    
    private MapTask mapTaskO ;
    private ReduceTask reduceTaskO ;
    private final ExecutorService mExecutor;
    /**
     * The {@link MasterServer} constructor.
     *
     * @param masterPort The port to listen on.
     * @param workers Information about each of the available workers in the
     *        system.
     */
    public MasterServer(int masterPort, List<WorkerInfo> workers) {
        mPort = masterPort;
        mWorkers = workers;
        mExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public void run() {
    	
    	//for the client
    	try {
			ServerSocket clientS = new ServerSocket(mPort);
			while (true){
				try{
					Socket skt = clientS.accept();
					mExecutor.execute(new MasterHelper(skt, mWorkers));
				}
				catch (Exception e){
					e.printStackTrace();
					break;
				}
			}
			
			try{
				clientS.close();
			}
			
			catch (Exception e1) {
				e1.printStackTrace();
			} 		
    	} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
    	finally{
    		mExecutor.shutdown();
    	}
    }
    

    /********************************************************************/
    /***************** STAFF CODE BELOW. DO NOT MODIFY. *****************/
    /********************************************************************/

    /**
     * Starts the master server on a distinct port. Information about each
     * available worker in the distributed system is parsed and passed as an
     * argument to the {@link MasterServer} constructor. This information can be
     * either specified via command line arguments or via system properties
     * specified in the <code>master.properties</code> and
     * <code>workers.properties</code> file (if no command line arguments are
     * specified).
     */
    public static void main(String[] args) {
    	System.out.println("arglength "+ args.length);
//    	for (int i=0; i<args.length; i++){
//    		System.out.println(args[i]);
//    	}
    	
        StaffUtils.makeMasterServer(args).start();
    }

}
