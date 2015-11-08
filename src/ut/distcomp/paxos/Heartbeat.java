package ut.distcomp.paxos;

import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Heartbeat extends Thread {

	public Heartbeat(Config config, NetController nc, int serverId,
			int startPrimaryLeader, BlockingQueue<Boolean> becomePrimary,
			int[] primaryLeaderView, AtomicInteger currentPrimaryLeader) {
		super();
		this.config = config;
		this.nc = nc;
		this.heartbeatQueue = nc.getHeartbeatQueue();
		this.serverId = serverId;
		this.becomePrimary = becomePrimary;
		exisitingTimers = new HashMap<>();
		timer = new Timer();
		timer2 = new Timer();
		currentPlId = currentPrimaryLeader;
		this.primaryLeaderView = primaryLeaderView;
		initializePrimaryView(startPrimaryLeader);
	}

	/**
	 * Sets its own value to the given init value and others to -1. Constructs
	 * others view based on the heartbeats it receives
	 * 
	 * @param initValue
	 */
	private void initializePrimaryView(int initValue) {
		for (int i = 0; i < config.numServers; i++) {
			if (i == serverId) {
				primaryLeaderView[i] = initValue;
			} else {
				primaryLeaderView[i] = -1;
			}
		}
	}

	public void recover() {
		config.logger.info("Retriving heartbeat..");
		heartbeatQueue.clear();
		try {
			Message m = heartbeatQueue.take();
			config.logger.info("Retriving heartbeat from " + m.toString());
			// Just adopt a value from any heartbeat.
			primaryLeaderView[serverId] = m.getPrimary();
			currentPlId.set(m.getPrimary());
		} catch (Exception e) {
			config.logger
					.severe("Error in heartbeat queue wait :" + e.getMessage());
			return;
		}
	}

	public void sendHeartBeat() {
		try {
			for (int dest = 0; dest < config.numServers; dest++) {
				if (dest == serverId) {
					continue;
				}
				Message m = new Message(serverId, dest);
				m.setHeartBeatContent(primaryLeaderView[serverId]);
				//config.logger.info("Sending heartbeat to " + dest);
				nc.sendMessageToServer(dest, m);
			}
		} catch (Exception e) {
			config.logger.severe(
					"Error in sending " + "heartbeat : " + e.getMessage());
		}
	}

	public void shutDown() {
		config.logger.info("Trying to kill heartbeat thread of " + serverId);
		killTimers();
		clearQueues();
	}

	private void killTimers() {
		tt.cancel();
		for (Integer key : exisitingTimers.keySet()) {
			exisitingTimers.get(key).cancel();
		}
		timer.cancel();
		timer2.cancel();
		config.logger.info("Shut down timers");
	}

	private void clearQueues() {
		heartbeatQueue.clear();
	}

	private void addTimerForServer(int i) {
		try {
			int delay = Config.HeartbeatConsumptionFrequency;
			TimerTask tti = new ElectNewLeaderOnTimeoutTask(i);
			exisitingTimers.put(i, tti);
			timer.schedule(tti, delay);
		} catch (Exception e) {
		}
	}

	// NOTE : This will block until a majority has elected some leader as
	// primary.
	class ElectNewLeaderOnTimeoutTask extends TimerTask {
		int failedProcess;

		public ElectNewLeaderOnTimeoutTask(int i) {
			failedProcess = i;
		}

		@Override
		public void run() {
			config.logger.info("Detected death of " + failedProcess + " and "
					+ "current primary is " + currentPlId);
			primaryLeaderView[failedProcess] = -1;
			if (failedProcess == currentPlId.get()) {
				config.logger.info(
						"Detected death of current leader " + currentPlId);
				primaryLeaderView[serverId] = (currentPlId.get() + 1)
						% config.numServers;
				config.logger.info("Setting vote for new leader to "
						+ primaryLeaderView[serverId]);
				int newLeader = waitForMajorityToElectNewLeader();
				config.logger.info("Majority elected leader as " + newLeader);
				primaryLeaderView[serverId] = newLeader;
				currentPlId.set(newLeader);
				if (newLeader == serverId) {
					if (becomePrimary.offer(true)) {
						config.logger.info("Elected as primary");
					} else {
						config.logger
								.info("Wasnt able to communicate to leader");
					}
				}

			} else {
				// Add a new timer even for a failed process.
				addTimerForServer(failedProcess);
			}
		}

	}

	class SendHeartBeatTask extends TimerTask {
		public void run() {
			try {
				sendHeartBeat();
			} catch (Exception e) {
				e.printStackTrace();
				// config.logger.log("", msg, thrown);
			}
		}
	}

	// Wait for a majority for any number other than -1 or the current
	// primary
	private int waitForMajorityToElectNewLeader() {
		boolean leaderSet = false;
		while (!leaderSet) {
			int possibleLeader = checkMajority();
			if (possibleLeader >= 0 && possibleLeader != currentPlId.get()) {
				leaderSet = true;
				return possibleLeader;
			}
		}
		return -1;
	}

	private int checkMajority() {
		int count = 0, i, majorityElement = -1;
		for (i = 0; i < config.numServers; i++) {
			if (count == 0)
				majorityElement = primaryLeaderView[i];
			if (primaryLeaderView[i] == majorityElement)
				count++;
			else
				count--;
		}
		count = 0;
		for (i = 0; i < config.numServers; i++)
			if (primaryLeaderView[i] == majorityElement) {
				count++;
			}
		if (count > config.numServers / 2) {
			return majorityElement;
		}
		return -1;
	}

	@Override
	public void run() {
		// Frequency at which heart beats are sent. Should not be too small
		// otherwise heart beat queue will start filling faster than the rate at
		// which it is consumed.
		long freq = 1000;
		// Set a new timer which executed the SendHeartbeatTask for the given
		// frequency.
		tt = new SendHeartBeatTask();
		timer2.schedule(tt, 0, freq);
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

	private void processHeartbeat() {
		Message m = null;
		try {
			m = heartbeatQueue.take();
		} catch (Exception e) {
			config.logger
					.severe("Error in heartbeat queue wait :" + e.getMessage());
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
		// config.logger.info("Set the primary value of "
		// + primaryLeaderView[m.getSrc()] + " to " + m.getPrimary());
	}

	/**
	 * Maintain the exisisting set of timers for all alive processes.
	 */
	HashMap<Integer, TimerTask> exisitingTimers;
	/**
	 * Timer to use for scheduling tasks.
	 */
	Timer timer;
	Timer timer2;
	// TimerTask to send heart beats
	TimerTask tt;
	private int serverId;
	private AtomicInteger currentPlId;
	private NetController nc;
	private Config config;
	private BlockingQueue<Message> heartbeatQueue;
	// In heartbeat this is the votes of all servers.
	// If its set to -1 then that process is dead
	private int[] primaryLeaderView;
	private BlockingQueue<Boolean> becomePrimary;
}
