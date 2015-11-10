package ut.distcomp.paxos;

import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Heartbeat extends Thread {

	public Heartbeat(Config config, NetController nc, int serverId,
			int startPrimaryLeader, BlockingQueue<Boolean> becomePrimary,
			int[] primaryLeaderView, IntegerWrapper currentPrimaryLeader) {
		super();
		this.config = config;
		this.nc = nc;
		this.heartbeatQueue = nc.getHeartbeatQueue();
		this.serverId = serverId;
		this.becomePrimary = becomePrimary;
		exisitingTimers = new HashMap<>();
		deathDetector = new Timer();
		heartBeatTimer = new Timer();
		heartBeatTask = new SendHeartBeatTask();
		currentPlId = currentPrimaryLeader;
		this.currentPrimary = primaryLeaderView;
		initializeCurrentPrimary(startPrimaryLeader);
	}

	class ElectPrimaryLeader extends TimerTask {
		int failedProcess;

		public ElectPrimaryLeader(int i) {
			failedProcess = i;
		}

		@Override
		public void run() {
			// synchronized (currentPlId) {
			config.logger.info("Detected death of " + failedProcess
					+ " and current primary is " + currentPlId.getValue());
			nc.shutDownOutgoingSocket(failedProcess);
			currentPrimary[failedProcess] = -1;
			if (failedProcess == currentPlId.getValue()) {
				setPrimary(getNextPrimary());
			} else {
				// Add a new timer even for a failed process since it might be
				// the next primary. So we will need to detect its death again.
				// NOTE : This redundancy is being introduced so that view of
				// primary does not diverge among the processes.
				// TODO(klad) : Figure out if this can be removed.
				addTimerForServer(failedProcess);
			}
			// }
		}

	}

	class SendHeartBeatTask extends TimerTask {
		public void run() {
			try {
				sendHeartBeat();
			} catch (Exception e) {
//				config.logger.severe(e.getMessage());
			}
		}
	}

	@Override
	public void run() {
		heartBeatTimer.schedule(heartBeatTask, 0, Config.HeartbeatFrequency);
		// Initialize all the timers to track heartbeat of other processes.
		for (int i = 0; i < config.numServers; i++) {
			if (i != serverId) {
				addTimerForServer(i);
			}
		}
		while (true) {
			processHeartbeat();
		}
	}

	public void recover() {
		config.logger.info("Retriving heartbeat..");
		heartbeatQueue.clear();
		try {
			Message m = heartbeatQueue.take();
			config.logger.info("Retriving heartbeat from " + m.toString());
			// Just adopt a value from any heartbeat.
			// NOTE : If process which is recovering is itself the primary then
			// becomePrimary.offer has to be executed after its leader thread
			// has been started.
			setPrimary(m.getPrimary());
		} catch (Exception e) {
			config.logger
					.severe("Error in heartbeat queue wait :" + e.getMessage());
			return;
		}
		config.logger.info("Finished recovery of heartbeat");
	}

	private void sendHeartBeat() {

		for (int dest = 0; dest < config.numServers; dest++) {
			if (dest == serverId) {
				continue;
			}
			try {
				Message m = new Message(serverId, dest);
				m.setHeartBeatContent(currentPrimary[serverId]);
//				config.logger.info("Sending heartbeat to " + dest + " at: "
//						+ System.currentTimeMillis());
				if (!nc.sendMessageToServer(dest, m)) {
//					config.logger.info("Failed to send HB to: " + dest);
				}
			} catch (Exception e) {
//				config.logger.severe(
//						"Error in sending " + "heartbeat : " + e.getMessage());
			}
		}
	}

	

	private void processHeartbeat() {
		Message m = null;
		try {
			m = heartbeatQueue.take();
		} catch (Exception e) {
//			config.logger
//					.severe("Error in heartbeat queue wait :" + e.getMessage());
			return;
		}
		int src = m.getSrc();
		// Disable the timer if a timer is running for that process.
		if (exisitingTimers.containsKey(src)) {
			exisitingTimers.get(src).cancel();
//			config.logger.info("Processed heart beat at: "
//					+ System.currentTimeMillis() + m.toString());
		}
		currentPrimary[src] = m.getPrimary();
		// synchronized (currentPlId) {
		if (currentPlId.getValue() != serverId && m.getPrimary() == serverId) {
			setPrimary(serverId);
		}
		// }
		addTimerForServer(src);
	}

	public void shutDown() {
		config.logger.info("Trying to kill heartbeat thread of " + serverId);
		killTimers();
		clearQueues();
	}

	private void killTimers() {
		heartBeatTask.cancel();
		for (Integer key : exisitingTimers.keySet()) {
			exisitingTimers.get(key).cancel();
		}
		deathDetector.cancel();
		heartBeatTimer.cancel();
		config.logger.info("Shut down timers");
	}

	private void clearQueues() {
		heartbeatQueue.clear();
	}

	private void addTimerForServer(int i) {
//		config.logger.info(
//				"Adding timer for: " + i + " at" + System.currentTimeMillis());
		try {
			TimerTask electLeader = new ElectPrimaryLeader(i);
			deathDetector.schedule(electLeader, Config.HeartbeatTimeout);
			exisitingTimers.put(i, electLeader);
		} catch (Exception e) {
//			config.logger.severe(e.getMessage());
		}
	}

	/**
	 * Sets its own value to the given init value and others to -1. Constructs
	 * others view based on the heartbeats it receives
	 * 
	 * @param initValue
	 */
	private void initializeCurrentPrimary(int initValue) {
		for (int i = 0; i < config.numServers; i++) {
			if (i == serverId) {
				currentPrimary[i] = initValue;
			} else {
				currentPrimary[i] = -1;
			}
		}
	}

	private void setPrimary(int primary) {
		if (primary == currentPlId.getValue()) {
			return;
		}
		config.logger.info("Setting new primary to " + primary);
		currentPrimary[serverId] = primary;
		currentPlId.setValue(primary);
		if (primary == serverId) {
			if (becomePrimary.offer(true)) {
//				config.logger.info("Elected as primary");
			} else {
//				config.logger.info("Leader ignored offer to become primary.\n"
//						+ "It must already have become primary");
			}
		}
	}

	private int getNextPrimary() {
		int nextPrimary = (currentPlId.getValue() + 1) % config.numServers;
		return nextPrimary;
	}

	/**
	 * Maintain the existing set of timers for all alive processes.
	 */
	HashMap<Integer, TimerTask> exisitingTimers;
	// Timer to detect death and schedule leader election on death detection.
	Timer deathDetector;
	// Timer to schedule periodic sending of heart beat.
	Timer heartBeatTimer;
	// TimerTask to send heart beats
	TimerTask heartBeatTask;
	private int serverId;
	private IntegerWrapper currentPlId;
	private NetController nc;
	private Config config;
	private BlockingQueue<Message> heartbeatQueue;
	// Array which maintains who is primary as per each process.
	// If its set to -1 then that process is dead.
	private int[] currentPrimary;
	private BlockingQueue<Boolean> becomePrimary;
}
