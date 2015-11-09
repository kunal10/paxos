package ut.distcomp.paxos;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Message.MessageType;
import ut.distcomp.paxos.Message.NodeType;

/**
 * Executes functionality of a acceptor.
 *
 */
public class Acceptor extends Thread {
	public Acceptor(Config config, NetController nc, int acceptorId) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = nc.getAcceptorQueue();
		this.acceptorId = acceptorId;
		this.accepted = new HashSet<PValue>();
		this.ballot = new Ballot(-1, -1);
	}

	// Interacts with other replicas to recover the lost state.
	public void recover() {
		config.logger.info("Retriving state for acceptor");
		// TODO : Should you run this in while loop till you get a recovery
		// message since there has to be one process which has to reply
		for (int i = 0; i < config.numServers; i++) {
			if (i != acceptorId) {
				sendStateRequest(i);
				Message recoverMessage = waitForStateResponse();
				if (recoverMessage != null) {
					config.logger.info("Retriving state from message "
							+ recoverMessage.toString());
					ballot = recoverMessage.getBallot();
					accepted = recoverMessage.getAccepted();
					break;
				}
			}
		}
		config.logger.info("Finished recovery for acceptor");
	}

	// TODO: Is poll okay ?
	private Message waitForStateResponse() {
		Message recoverMsg = null;
		try {
			recoverMsg = queue.poll(Config.QueuePollTimeout,
					TimeUnit.MILLISECONDS);
			while (recoverMsg.getMsgType() != MessageType.STATE_RES) {
				recoverMsg = queue.poll(Config.QueuePollTimeout,
						TimeUnit.MILLISECONDS);
			}
		} catch (Exception e) {
			config.logger
					.severe("Interrupted while receiving " + "replica state");
		}
		return recoverMsg;
	}

	private void sendStateRequest(int dest) {
		Message m = new Message(acceptorId, dest);
		m.setStateRequestContent(NodeType.ACCEPTOR);
		if (!nc.sendMessageToServer(dest, m)) {
			config.logger.info(
					"Acceptor : Send of state request to " + dest + " failed");
		} else {
			config.logger.info("Acceptor : Send of state request to " + dest
					+ " successful");
		}
	}

	public void run() {
		while (true) {
			Message m = null;
			try {
				m = queue.take();
			} catch (Exception e) {
				config.logger.severe(e.getMessage());
				return;
			}
			switch (m.getMsgType()) {
			case STATE_RES:
				config.logger.info(
						"Ignoring Received STATE_RES msg:" + m.toString());
				break;
			case STATE_REQ:
				config.logger.info("Received STATE_REQ msg:" + m.toString());
				Message response = new Message(acceptorId, m.getSrc());
				response.setStateResponseContent(NodeType.ACCEPTOR, accepted,
						ballot);
				config.logger
						.info("Sending STATE_RES msg:" + response.toString());
				nc.sendMessageToServer(m.getSrc(), response);
				break;
			case P1A:
				Ballot b = m.getBallot();
				Message msg = new Message(acceptorId, b.getlId());
				config.logger.info("Received P1A msg:" + m.toString());
				if (b != null && b.compareTo(ballot) == 1) {
					ballot = b;
				}
				msg.setP1BContent(ballot, accepted, m.getThreadId());
				config.logger.info("Sending P1B msg:" + msg.toString());
				nc.sendMessageToServer(b.getlId(), msg);
				break;
			case P2A:
				Ballot b2 = m.getBallot();
				Message msg2 = new Message(acceptorId, b2.getlId());
				config.logger.info("Received P2A msg:" + m.toString());
				if (b2 != null && b2.compareTo(ballot) >= 0) {
					ballot = b2;
					try {
						accepted.add(new PValue(ballot, m.getsValue()));
					} catch (Exception e) {
						config.logger.severe("Error receive P2A  : Server ID :"
								+ acceptorId + " Message :" + m.toString());
					}

				}
				msg2.setP2BContent(ballot, m.getThreadId());
				config.logger.info("Sending P2B msg:" + msg2.toString());
				nc.sendMessageToServer(b2.getlId(), msg2);
				break;
			default:
				config.logger.severe("Received Unexpected Msg" + m.toString());
				break;
			}
		}
	}

	private int acceptorId;
	private Ballot ballot;
	private Set<PValue> accepted;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
}
