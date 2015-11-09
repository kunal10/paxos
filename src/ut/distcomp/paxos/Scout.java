package ut.distcomp.paxos;

import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Message.NodeType;

public class Scout extends Thread {
	public Scout(Config config, NetController nc, int[] aliveSet,
			AtomicInteger numMsgsToSend, int leaderId, int scoutId, Ballot b) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = nc.getScoutQueue(scoutId);
		this.leaderId = leaderId;
		this.scoutId = scoutId;
		this.b = new Ballot(b);
		this.aliveSet = aliveSet;
		this.numMsgsToSend = numMsgsToSend;
		this.timer = null;
	}

	public void run() {
		Set<Integer> received = new HashSet<Integer>();
		Set<PValue> accepted = new HashSet<PValue>();
		BooleanWrapper timeout = new BooleanWrapper(false);
		// Send P1A message to all acceptors.
		sendP1AToAcceptors();
		// TODO(asvenk) : Add a timer for this thread.
		// When timer times out send a blocked message.
		while (!Leader.isBlocked(aliveSet, received,
				config.numServers / 2 + 1)) {
			// Start the timer if its not already started.
			startTimer(timeout);
			Message m = null;
			try {
				m = queue.take();
			} catch (Exception e) {
				config.logger.severe(e.getMessage());
				return;
			}
			Ballot b1 = m.getBallot();
			if (b1 == null) {
				config.logger.severe(
						"Received msg without any ballot:" + m.toString());
				continue;
			}
			Message msg = new Message(leaderId, leaderId);
			switch (m.getMsgType()) {
			case P1B:
				if (b1.equals(b)) {
					accepted.addAll(m.getAccepted());
					received.add(m.getSrc());
					// If received majority.
					if (received.size() >= (config.numServers / 2 + 1)) {
						// Send the adopted msg to leader.
						msg.setAdoptedContent(b, accepted);
						config.logger.info(
								"Sending Adopted to leader:" + msg.toString());
						nc.sendMessageToServer(leaderId, msg);
						return;
					}
				} else {
					// Send PreEmpted message.
					sendPreEmptedToLeader(b1);
					return;
				}
				break;
			default:
				config.logger.severe("Received Unexpected Msg" + m.toString());
				break;
			}
		}
		// Send blocked message to the leader.
		sendBlockedToLeader();
	}

	private void sendP1AToAcceptors() {
		config.logger.info(
				"Sending P1A msg to all Acceptors for ballot:" + b.toString());
		Message msg = null;
		for (int acceptorId = 0; acceptorId < config.numServers; acceptorId++) {
			synchronized (numMsgsToSend) {
				int curValue = numMsgsToSend.get();
				if (curValue != -1) {
					if (curValue == 0) {
						config.logger.info("Not sending any more P2A messages. "
								+ "\nWaiting to be killed");
						break;
					}
				}
				msg = new Message(leaderId, acceptorId);
				msg.setP1AContent(b, scoutId);
				nc.sendMessageToServer(acceptorId, msg);
				// Decrement the number of messages to be sent.
				if (curValue != -1) {
					numMsgsToSend.decrementAndGet();
				}
			}
		}
	}

	private void sendPreEmptedToLeader(Ballot b1) {
		config.logger.info(String.format(
				"Sending PreEmpted message from scout %d to leader %d: ",
				scoutId, leaderId));
		Message msg = new Message(leaderId, leaderId);
		msg.setPreEmptedContent(NodeType.SCOUT, b1);
		nc.sendMessageToServer(leaderId, msg);
	}

	private void sendBlockedToLeader() {
		config.logger.info(String.format(
				"Sending Blocked message from scout %d to leader %d: ", scoutId,
				leaderId));
		Message msg = new Message(leaderId, leaderId);
		msg.setBlockedContent(NodeType.SCOUT, scoutId);
		nc.sendMessageToServer(leaderId, msg);
	}

	// Start the timer if its not already started.
	private void startTimer(BooleanWrapper timeout) {
		if (timer == null) {
			timer = new Timer();
			TimeoutUtil tu = new TimeoutUtil(timeout);
			timer.schedule(tu, 2000);
		}
	}

	private int leaderId;
	private int scoutId;
	private Ballot b;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
	private int[] aliveSet;
	private AtomicInteger numMsgsToSend;
	private Timer timer;
}
