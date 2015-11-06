package ut.distcomp.paxos;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
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
		for (int i = 0; i < config.numServers; i++) {
			if (i != acceptorId) {
				Message m = new Message(acceptorId, i);
				m.setStateRequestContent(NodeType.ACCEPTOR);
				if (nc.sendMessageToServer(i, m)) {
					try {
						Message recoverMessage = queue.take();
						config.logger
								.info("Received a acceptor recovery message "
										+ "" + recoverMessage.toString());
						Ballot b = recoverMessage.getBallot();
						config.logger.info("Received Ballot : " + b.toString());
						// Build a increasing set while recovery.
						if (b.compareTo(ballot) > 0) {
							ballot = b;
							accepted = recoverMessage.getAccepted();
						}

					} catch (InterruptedException e) {
						config.logger.severe("Interrupted while waiting for "
								+ "" + "acceptor recovery");
					}
				}
			}
		}
	}

	public void run() {
		while (true) {
			Message m = null;
			try {
				m = queue.take();
			} catch (Exception e) {
				config.logger.severe(e.getMessage());
				continue;
			}
			switch (m.getMsgType()) {
			case STATE_RES:
				config.logger.info(
						"Ignoring Received STATE_RES msg:" + m.toString());
				config.logger.info("Shouldn't have received at this point");
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
				msg.setP1BContent(ballot, accepted, msg.getThreadId());
				config.logger.info("Sending P1A msg:" + msg.toString());
				nc.sendMessageToServer(b.getlId(), msg);
				break;
			case P2A:
				Ballot b2 = m.getBallot();
				Message msg2 = new Message(acceptorId, b2.getlId());
				config.logger.info("Received P2A msg:" + m.toString());
				if (b2 != null && b2.compareTo(ballot) >= 0) {
					ballot = b2;
					accepted.add(new PValue(ballot, msg2.getsValue()));
				}
				msg2.setP2BContent(ballot, m.getThreadId());
				config.logger.info("Sending P2A msg:" + msg2.toString());
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
