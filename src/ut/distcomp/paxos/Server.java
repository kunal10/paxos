package ut.distcomp.paxos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Message.NodeType;

/**
 * 
 * Encompasses of a leader, replica and an acceptor.
 *
 */
public class Server {

	public Server(int serverId, Config config) {
		super();
		this.serverId = serverId;
		this.numOfClients = config.numOfClients;
		this.numOfServers = config.numOfServers;
		this.config = config;
		this.leaderQueue = new LinkedBlockingQueue<>();
		this.replicaQueue = new LinkedBlockingQueue<>();
		this.acceptorQueue = new LinkedBlockingQueue<>();
		this.heartbeatQueue = new LinkedBlockingQueue<>();
		this.commanderQueue = new HashMap<>();
		this.scoutQueue = new HashMap<>();
		this.setLeaderToPrimary = new SynchronousQueue<>();
		this.nc = new NetController(config, leaderQueue, replicaQueue, 
				acceptorQueue, commanderQueue, scoutQueue,
				heartbeatQueue);
		this.aliveSet = new int[config.numOfServers];
	}

	/**
	 * Kill all threads and exit.
	 */
	public void CrashServer() {
		nc.shutdown();
		heartbeatThread.shutDown();
		killThread(heartbeatThread);
		killThread(leaderThread);
		killThread(replicaThread);
		killThread(acceptorThread);
	}

	private void killThread(Thread t) {
		if (t != null) {
			t.stop();
		}
	}

	/**
	 * Start the server.
	 */
	public void StartServer() {
		initializeServerThreads();
		startServerThreads();
		// If the id is 0 insert into the boolean blocking queue ?
	}

	/**
	 * Revive a server.
	 */
	public void RestartServer(){
		config.logger.info("Recovering..");
		initializeServerThreads();
		config.logger.info("Initialize Threads");
		recoverServerState();
		config.logger.info("Set recovered state");
		startServerThreads();
		config.logger.info("Start Threads");
	}

	// Retrieve state for Replica.
	private void recoverServerState(){
		try {
			Thread.sleep(Config.QueueTimeoutVal);
		} catch (InterruptedException e) {
		}
		// Retrieve state for replica.
		replicaThread.recover();
		config.logger.info("Retrived state for replica");
		// Retrieve state for acceptor.
		acceptorThread.recover();
		config.logger.info("Retrived state for acceptor");
		// Retrieve a vote for the heartbeat thread.
		heartbeatThread.recover();
		config.logger.info("Retrived state for heartbeat");
		// TODO: Send all messages from replica to leader ?
		// TODO: Clear Queues ?
		/* TODO: To make this asynchronous : 
		 * All the threads should have a variable called recovery to be set. 
		 * If set they call the recover function on start of the thread. 
		 * Also there is a state maintained in the server IsRecovering which 
		 * should be set to false once all threads have recovered. 
		 * This state should be used by allclear in blocking
		 * */ 
	}

	private void initializeServerThreads() {
		replicaThread = new Replica(config, nc, serverId, replicaQueue);
		acceptorThread = new Acceptor(config, nc, serverId);
		heartbeatThread = new Heartbeat(config, nc, serverId, 0, 
				setLeaderToPrimary, aliveSet);
		// TODO: Initialize and Pass the atomic integer to the leader here.
	}

	private void startServerThreads() {
		replicaThread.start();
		heartbeatThread.start();
		acceptorThread.start();
		// TODO: Start leader thread.
	}
	
	public boolean IsServerAlive(){
		boolean isAlive = false;
		if(heartbeatThread != null && replicaThread != null && 
				leaderThread != null && acceptorThread != null){
			isAlive = heartbeatThread.isAlive() && replicaThread.isAlive() && 
					leaderThread.isAlive() && acceptorThread.isAlive();
		}
		return isAlive;
	}
	
	public boolean IsServerExecutingProtocol(){
		boolean isExecuting = false;
		// TODO: check if there are any existing commander and scout threads.
		// Return true is there are threads.
		return isExecuting;
	}

	/**
	 * Kill the server after specified no of messages if you are currently the
	 * primary leader.
	 * 
	 * @param n
	 */
	public void timeBombLeader(int n) {
		// TODO: Implement this.
		if(isPrimaryLeader){
			// A number which is shared between two threads.
			// Reset this number here.
			// Both commander and scout increase this number while sending messages.
			// Spawn a thread which checks whether this number is equal to n. 
			// As soon as its equal call kill()
			numberOfMessagesToTimebomb.set(0);
			Thread t = new Thread(){
				@Override
				public void run() {
					while(numberOfMessagesToTimebomb.get() <= n){
						// Do nothing
					}
					CrashServer();
				}
			};
			t.start();
			
		}
	}

	/**
	 * Server Id of this server instance.
	 */
	final private int serverId;
	/**
	 * Num of clients in the system.
	 */
	final private int numOfClients;
	/**
	 * Num of servers in the system.
	 */
	final private int numOfServers;
	/**
	 * Communication framework for communicating.
	 */
	final private NetController nc;
	/**
	 * Config corresponding to this Server.
	 */
	final private Config config;
	/**
	 * Reference to the leader thread of this server.
	 */
	private Leader leaderThread;
	/**
	 * Reference to the replica thread of this server.
	 */
	private Replica replicaThread;
	/**
	 * Reference to the acceptor thread of this server.
	 */
	private Acceptor acceptorThread;
	/**
	 * Reference to the heartbeat thread of this server.
	 */
	private Heartbeat heartbeatThread;
	/**
	 * Reference to the leader queue of this server.
	 */
	BlockingQueue<Message> leaderQueue;
	/**
	 * Reference to the replica queue of this server.
	 */
	BlockingQueue<Message> replicaQueue;
	/**
	 * Reference to the acceptor queue of this server.
	 */
	BlockingQueue<Message> acceptorQueue;
	/**
	 * Reference to the heartbeat queue of this server.
	 */
	BlockingQueue<Message> heartbeatQueue;
	/**
	 * Reference to the list of commander queues
	 */
	HashMap<Integer, BlockingQueue<Message>> commanderQueue;
	/**
	 * Reference to list of scout queues.
	 */
	HashMap<Integer, BlockingQueue<Message>> scoutQueue;

	BlockingQueue<Boolean> setLeaderToPrimary;

	int[] aliveSet;
	
	boolean isPrimaryLeader;
	
	AtomicInteger numberOfMessagesToTimebomb;
}
