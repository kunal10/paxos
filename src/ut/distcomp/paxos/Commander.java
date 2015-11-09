package ut.distcomp.paxos;

import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Message.NodeType;

public class Commander extends Thread {
	public Commander(Config config, NetController nc, int[] aliveSet,
			AtomicInteger numMsgsToSend, int leaderId, int commanderId,
			PValue pValue) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = nc.getCommanderQueue(commanderId);
		this.leaderId = leaderId;
		this.commanderId = commanderId;
		this.pValue = new PValue(pValue);
		this.aliveSet = aliveSet;
		this.numMsgsToSend = numMsgsToSend;
		this.timer = null;
	}

	public void run() {
		Set<Integer> received = new HashSet<Integer>();
		BooleanWrapper timeout = new BooleanWrapper(false);
		// Send P2A message to all acceptors.
		sendP2AToAcceptors();

		// TODO(asvenk) : Add a timer for this thread.
		// When timer times out send a blocked message.
		while (!Leader.isBlocked(aliveSet, received, config.numServers / 2 + 1)
				&& !timeout.getValue()) {
			// Start the timer if its not already started.
			startTimer(timeout);
			Message m = null;
			try {
				m = queue.take();
			} catch (Exception e) {
				config.logger.severe(e.getMessage());
				return;
			}
			Ballot b = pValue.getBallot();
			Ballot b1 = m.getBallot();
			if (b1 == null) {
				config.logger.severe(
						"Received msg without any ballot:" + m.toString());
				continue;
			}
			switch (m.getMsgType()) {
			case P2B:
				if (b1.equals(b)) {
					received.add(m.getSrc());
					// If received majority
					if (received.size() >= (config.numServers / 2 + 1)) {
						// Send the decision to all replicas.
						sendDecisionToReplicas();

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

	private void sendP2AToAcceptors() {
		config.logger.info("Sending P2A msg to all Acceptors for pvalue:"
				+ pValue.toString());
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
				msg.setP2AContent(pValue, commanderId);
				nc.sendMessageToServer(acceptorId, msg);
				// Decrement the number of messages to be sent.
				if (curValue != -1) {
					numMsgsToSend.decrementAndGet();
				}
			}
		}
	}

	private void sendDecisionToReplicas() {
		config.logger
				.info("Sending Decision to all replicas:" + pValue.toString());
		Message msg = null;
		for (int replicaId = 0; replicaId < config.numServers; replicaId++) {
			msg = new Message(leaderId, replicaId);
			msg.setDecisionContent(pValue.getsValue());
			nc.sendMessageToServer(replicaId, msg);
		}
	}

	private void sendPreEmptedToLeader(Ballot b1) {
		config.logger.info(String.format(
				"Sending PreEmpted message from commader %d to leader %d: ",
				commanderId, leaderId));
		Message msg = new Message(leaderId, leaderId);
		msg.setPreEmptedContent(NodeType.COMMANDER, b1);
		nc.sendMessageToServer(leaderId, msg);
	}

	private void sendBlockedToLeader() {
		config.logger.info(String.format(
				"Sending Blocked message from commader %d to leader %d: ",
				commanderId, leaderId));
		Message msg = new Message(leaderId, leaderId);
		msg.setBlockedContent(NodeType.COMMANDER, commanderId);
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
	private int commanderId;
	private PValue pValue;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
	private int[] aliveSet;
	private AtomicInteger numMsgsToSend;
	private Timer timer;
}
