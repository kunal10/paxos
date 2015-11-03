package ut.distcomp.paxos;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Heartbeat implements Runnable {

	public Heartbeat(Config config, NetController nc, int serverId, 
			int startPrimaryLeader,
			BlockingQueue<Boolean> setLeaderToPrimary) {
		super();
		this.config = config;
		this.nc = nc;
		this.heartbeatQueue = nc.getHeartbeatQueue();
		this.serverId = serverId;
		this.setLeaderToPrimary = setLeaderToPrimary;
		exisitingTimers = new HashMap<>();
		timer = new Timer();
		currentPlId = startPrimaryLeader;
		primaryLeaderView = new int[config.numOfServers];
		initializePrimaryView(startPrimaryLeader);
	}

	private void initializePrimaryView(int initValue) {
		for (int i = 0; i < config.numOfServers; i++) {
			if (i == serverId) {
				primaryLeaderView[i] = initValue;
			} else {
				primaryLeaderView[i] = -1;
			}
		}
	}

	public void sendHeartBeat() {
		try {
			for (int dest = 0; dest < config.numOfServers; dest++) {
				if (dest == serverId) {
					continue;
				}
				Message m = new Message(serverId, dest);
				m.setHeartBeatContent(primaryLeaderView[serverId]);
			}
		} catch (Exception e) {
			config.logger.log(Level.SEVERE, "Error in sending "
					+ "heartbeat : " + e.getMessage());
		}
	}

	public void shutDown() {
		killTimers();
		clearQueues();
	}

	private void killTimers() {
		tt.cancel();
		for (Integer key : exisitingTimers.keySet()) {
			exisitingTimers.get(key).cancel();
		}
		timer.cancel();
		config.logger.info("Shut down timers");
	}

	private void clearQueues() {
		heartbeatQueue.clear();
	}

	private void addTimerForServer(int i) {
		try {
			int delay = 1200;
			TimerTask tti = new ElectNewLeaderOnTimeoutTask(i);
			exisitingTimers.put(i, tti);
			timer.schedule(tti, delay);
		} catch (Exception e) {
		}
	}

	class ElectNewLeaderOnTimeoutTask extends TimerTask {
		int failedProcess;

		public ElectNewLeaderOnTimeoutTask(int i) {
			failedProcess = i;
		}

		@Override
		public void run() {
			primaryLeaderView[failedProcess] = -1;
			if (failedProcess == currentPlId) {
				config.logger.info("Detected death of current leader " + currentPlId);
				primaryLeaderView[serverId] = 
						(currentPlId + 1) % config.numOfServers;
				config.logger.info("Setting vote for new leader to " + 
						primaryLeaderView[serverId]);
				int newLeader = waitForMajorityToElectNewLeader();
				config.logger.info("Majority elected leader as " + newLeader);
				primaryLeaderView[serverId] = newLeader;
				currentPlId = newLeader;
				if (newLeader == serverId) {
					config.logger.info("The elected leader matches my "
							+ "server ID");
					if (setLeaderToPrimary.offer(true)) {
						config.logger.info("Informed my leader to become "
								+ "primary leader");
					} else {
						config.logger.info("Wasnt able to communicate to "
								+ "my leader");
					}
				}

			} else {
				// Add a new timer even for a failed process.
				addTimerForServer(failedProcess);
			}
		}

		// Wait for a majority for any number other than -1 or the current
		// primary
		private int waitForMajorityToElectNewLeader() {
			boolean leaderSet = false;
			while (!leaderSet) {
				int possibleLeader = checkMajority();
				if (possibleLeader >= 0 && possibleLeader != currentPlId) {
					leaderSet = true;
					return possibleLeader;
				}
			}
			return -1;
		}

		private int checkMajority() {
			int count = 0, i, majorityElement = -1;
			for (i = 0; i < config.numOfServers; i++) {
				if (count == 0)
					majorityElement = primaryLeaderView[i];
				if (primaryLeaderView[i] == majorityElement)
					count++;
				else
					count--;
			}
			count = 0;
			for (i = 0; i < config.numOfServers; i++)
				if (primaryLeaderView[i] == majorityElement)
					count++;
			if (count > config.numOfServers / 2)
				return majorityElement;
			return -1;
		}
	}

	class SendHeartBeatTask extends TimerTask {
		public void run() {
			try {
				sendHeartBeat();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void run() {
		// Frequency at which heart beats are sent. Should not be too small
		// otherwise heart beat queue will start filling faster than the rate at
		// which it is consumed.
		long freq = 1000;
		// Set a new timer which executed the SendHeartbeatTask for the given
		// frequency.
		// TODO:On kill this timer should be killed using tt.cancel();
		// timer.cancel();
		// TODO: Register timer threads for killing later.
		tt = new SendHeartBeatTask();
		timer.schedule(tt, 0, freq);
		// Initialize all the timers to track heartbeat of other processes.
		for (int i = 0; i < config.numOfServers; i++) {
			if (i != serverId) {
				addTimerForServer(i);
			}
		}
		while (true) {
			processHeartbeat();
		}
	}

	private void processHeartbeat() {
		Message m = null;
		try {
			m = heartbeatQueue.take();
		} catch (InterruptedException e) {
			config.logger.log(Level.SEVERE, "Error in waiting on heartbeat"
					+ " queue : " + e.getMessage());
			return;
		}
		// Disable the timer if a timer is running for that process.
		if (exisitingTimers.containsKey(m.getSrc())) {
			// config.logger.info("Received HB msg : " + m.toString());
			exisitingTimers.get(m.getSrc()).cancel();
		}
		// Add a new timer for the process which has sent a heartbeat
		// config.logger.info(pId + " Adding timer for "+m.getSrc());
		addTimerForServer((m.getSrc()));
		primaryLeaderView[m.getSrc()] = m.getPrimary();
	}

	/**
	 * Maintain the exisisting set of timers for all alive processes.
	 */
	HashMap<Integer, TimerTask> exisitingTimers;
	/**
	 * Timer to use for scheduling tasks.
	 */
	Timer timer;
	// TimerTask to send heart beats
	TimerTask tt;
	final private Config config;
	final private NetController nc;
	private BlockingQueue<Message> heartbeatQueue;
	private int currentPlId;
	private int[] primaryLeaderView;
	final private int serverId;
	private BlockingQueue<Boolean> setLeaderToPrimary;
}
