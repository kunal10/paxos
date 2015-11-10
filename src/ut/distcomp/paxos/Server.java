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
		this.currentPrimaryLeader = new IntegerWrapper(0);
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
		killThread(heartbeatThread);
		heartbeatQueue = null;
		if (leaderThread != null) {
			leaderThread.killAllScoutsAndCommander();
		}
		killThread(leaderThread);
		leaderThread = null;
		killThread(replicaThread);
		replicaThread = null;
		killThread(acceptorThread);
		acceptorThread = null;
		try {
			Thread.sleep(Config.HeartbeatFrequency);
		} catch (InterruptedException e) {
		}
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
			Thread.sleep(Config.RevivalDelay);
		} catch (InterruptedException e) {
		}
		// Retrieve state for replica.
		replicaThread.recover();
		// Retrieve state for acceptor.
		acceptorThread.recover();
		// Retrieve a vote for the heartbeat thread.
		heartbeatThread.recover();
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
		// Wait for leader thread to start.
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			config.logger.severe(e.getMessage());
		}
		if (serverId == currentPrimaryLeader.getValue()) {
			if (becomePrimary.offer(true)) {
				config.logger.info("Becoming primary on start/recovery");
			} else {
				config.logger
						.severe("Could not become primary on start/recovery");
			}
		}
	}

	public boolean IsServerAlive() {
		boolean isAlive = false;
		config.logger.info("Is server alive");
		if (isThreadNull(heartbeatThread)) {
			config.logger.info(serverId + " Heartbeat thread is null");
		}
		if (isThreadNull(replicaThread)) {
			config.logger.info(serverId + " Replica thread is null");
		}
		if (isThreadNull(leaderThread)) {
			config.logger.info(serverId + " Leader thread is null");
		}
		if (isThreadNull(acceptorThread)) {
			config.logger.info(serverId + " Acceptor thread is null");
		}
		if (heartbeatThread != null && replicaThread != null
				&& leaderThread != null && acceptorThread != null) {
			config.logger.info("Is Alive values :H" + heartbeatThread.isAlive()
					+ ":R:" + replicaThread.isAlive() + ":L:"
					+ leaderThread.isAlive() + ":A:"
					+ acceptorThread.isAlive());
			isAlive = heartbeatThread.isAlive() && replicaThread.isAlive()
					&& leaderThread.isAlive() && acceptorThread.isAlive();
		}
		return isAlive;
	}

	private boolean isThreadNull(Thread t) {
		if (t == null) {
			return true;
		}
		return false;
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
		if (currentPrimaryLeader.getValue() == serverId) {
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

	IntegerWrapper currentPrimaryLeader;

	int[] aliveSet;

	// Number of messages to be sent as primary before crashing the server.
	// NOTE : Only P2A and P2B messages are counted.
	private AtomicInteger numMsgsToSend;
}
