package ut.distcomp.paxos;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

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
		this.nc = new NetController(config, leaderQueue, 
				replicaQueue, acceptorQueue, commanderQueue, scoutQueue, 
				heartbeatQueue);
		this.aliveSet = new int[config.numOfServers];
	}

	/**
	 * Kill all threads and exit.
	 */
	public void CrashServer(){
		nc.shutdown();
		heartbeatThread.shutDown();
		killThread(heartbeatThread);
		killThread(leaderThread);
		killThread(replicaThread);
		killThread(acceptorThread);
	}
	
	private void killThread(Thread t){
		if(t!=null){
			t.stop();
		}
	}
	
	/**
	 * Revive a server.
	 */
	public void RestartServer(){
		
	}
	
	/**
	 * Start the server.
	 */
	public void StartServer(){
		replicaThread = new Thread(new Replica(config, nc, serverId, 
				replicaQueue));
		replicaThread.start();
		heartbeatThread = (new Heartbeat(config, nc, serverId, 0, 
				setLeaderToPrimary, aliveSet));
		heartbeatThread.start();
	}
	
	/**
	 * Kill the server after specified no of messages if you are currently the 
	 * primary leader.
	 * @param n
	 */
	public void timeBombLeader(int n){
		
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
	private Thread leaderThread;
	/**
	 * Reference to the replica thread of this server.
	 */
	private Thread replicaThread;
	/**
	 * Reference to the acceptor thread of this server.
	 */
	private Thread acceptorThread;
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
}
