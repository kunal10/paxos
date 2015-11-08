package ut.distcomp.paxos;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Server {

	public Server(int serverId, Config config) {
		super();
		this.serverId = serverId;
		this.config = config;
		this.leaderQueue = new LinkedBlockingQueue<>();
		this.replicaQueue = new LinkedBlockingQueue<>();
		this.acceptorQueue = new LinkedBlockingQueue<>();
		this.heartbeatQueue = new LinkedBlockingQueue<>();
		this.commanderQueues = new HashMap<>();
		this.scoutQueues = new HashMap<>();
		this.becomePrimary = new SynchronousQueue<>();
		this.currentPrimaryLeader = new AtomicInteger(0);
		this.nc = new NetController(config, leaderQueue, replicaQueue,
				acceptorQueue, commanderQueues, scoutQueues, heartbeatQueue);
		this.aliveSet = new int[config.numServers];
		this.numMsgsToSend = new AtomicInteger(-1);
	}

	/**
	 * Kill all threads and exit.
	 */
	public void CrashServer() {
		nc.shutdown();
		heartbeatThread.shutDown();
		leaderThread.killAllScoutsAndCommander();
		killThread(heartbeatThread);
		heartbeatQueue = null;
		killThread(leaderThread);
		leaderThread = null;
		killThread(replicaThread);
		replicaThread = null;
		killThread(acceptorThread);
		acceptorThread = null;
		// TODO: Kill any timebomb leader thread.

	}

	private void killThread(Thread t) {
		if (t != null) {
			t.stop();
		}
		t = null;
	}

	/**
	 * Start the server.
	 */
	public void StartServer() {
		initializeServerThreads();
		startServerThreads();
		try {
			Thread.sleep(100);
		} catch (InterruptedException e1) {
		}
		if (serverId == 0) {
			if (becomePrimary.offer(true)) {
				config.logger.info("Set primary to 0 on start of the system");
			} else {
				config.logger.info(
						"Did not set primary to 0 on start of the system:");
			}
		}
	}

	/**
	 * Revive a server.
	 */
	public void RestartServer() {
		config.logger.info("Recovering..");
		initializeServerThreads();
		config.logger.info("Initialize Threads");
		recoverServerState();
		config.logger.info("Set recovered state");
		startServerThreads();
		config.logger.info("Start Threads");
	}

	// Retrieve state for Replica.
	private void recoverServerState() {
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
		/*
		 * TODO*: To make this asynchronous : All the threads should have a
		 * variable called recovery to be set. If set they call the recover
		 * function on start of the thread. Also there is a state maintained in
		 * the server IsRecovering which should be set to false once all threads
		 * have recovered. This state should be used by allclear in blocking.
		 */
	}

	private void initializeServerThreads() {
		replicaThread = new Replica(config, nc, serverId, replicaQueue);
		acceptorThread = new Acceptor(config, nc, serverId);
		heartbeatThread = new Heartbeat(config, nc, serverId, 0, becomePrimary,
				aliveSet, currentPrimaryLeader);
		leaderThread = new Leader(config, nc, aliveSet, numMsgsToSend,
				becomePrimary, serverId);
	}

	private void startServerThreads() {
		replicaThread.start();
		heartbeatThread.start();
		acceptorThread.start();
		leaderThread.start();
	}

	public boolean IsServerAlive() {
		boolean isAlive = false;
		if (heartbeatThread != null && replicaThread != null
				&& leaderThread != null && acceptorThread != null) {
			isAlive = heartbeatThread.isAlive() && replicaThread.isAlive()
					&& leaderThread.isAlive() && acceptorThread.isAlive();
		}
		return isAlive;
	}

	public boolean IsServerExecutingProtocol() {
		return !leaderThread.areScoutsCommandersDead();
		// check if there are any existing commander and scout threads.
		// Return true is there are threads.
	}

	/**
	 * Kill the server after specified no of messages if you are currently the
	 * primary leader.
	 * 
	 * @param n
	 */
	public void timeBombLeader(int n) {
		if (currentPrimaryLeader.get() == serverId) {
			numMsgsToSend.set(n);
			Thread t = new Thread() {
				@Override
				public void run() {
					while (numMsgsToSend.get() > 0) {
						// Wait for primary to send n messages.
						try {
							Thread.sleep(10);
						} catch (Exception e) {
							config.logger.info(e.getMessage());
						}
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
	HashMap<Integer, BlockingQueue<Message>> commanderQueues;
	/**
	 * Reference to list of scout queues.
	 */
	HashMap<Integer, BlockingQueue<Message>> scoutQueues;

	SynchronousQueue<Boolean> becomePrimary;

	AtomicInteger currentPrimaryLeader;

	int[] aliveSet;

	// Number of messages to be sent as primary before crashing the server.
	// NOTE : Only P2A and P2B messages are counted.
	AtomicInteger numMsgsToSend;
}
