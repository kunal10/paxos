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
	}

	// Interacts with other replicas to recover the lost state.
	public void recover() {
		for (int i = 0; i < config.numOfServers; i++) {
			Message m = new Message(acceptorId, i);
			m.setStateRequestContent(NodeType.ACCEPTOR);
			if (nc.sendMessageToServer(i, m)) {
				try {
					Message recoverMessage = queue.take();
					Ballot b = recoverMessage.getBallot();
					// Build a increasing set while recovery.
					if (b.compareTo(ballot) > 0) {
						ballot = b;
						accepted = recoverMessage.getAccepted();
					}

				} catch (InterruptedException e) {
					config.logger.severe("Interrupted while waiting for " + "acceptor recovery");
				}
			}
		}
	}

	public void run() {
		while (true) {
			Message m = null;
			try {
				m = queue.take();
			} catch (InterruptedException e) {
				config.logger.severe(e.getMessage());
				continue;
			}
			Ballot b = m.getBallot();
			Message msg = new Message(acceptorId, b.getlId());
			switch (m.getMsgType()) {
			case STATE_RES:
				config.logger.info("Ignoring Received STATE_RES msg:" + m.toString());
				config.logger.info("Shouldn't have received at this point");
				break;
			case STATE_REQ:
				config.logger.info("Received STATE_REQ msg:" + m.toString());
				Message response = new Message(acceptorId, m.getSrc());
				response.setStateResponseContent(NodeType.ACCEPTOR, accepted, ballot);
				config.logger.info("Sending STATE_RES msg:" + response.toString());
				nc.sendMessageToServer(m.getSrc(), response);
				break;
			case P1A:
				config.logger.info("Received P1A msg:" + m.toString());
				if (b != null && b.compareTo(ballot) == 1) {
					ballot = b;
				}
				msg.setP1BContent(ballot, accepted, msg.getThreadId());
				config.logger.info("Sending P1A msg:" + msg.toString());
				nc.sendMessageToServer(b.getlId(), msg);
				break;
			case P2A:
				config.logger.info("Received P2A msg:" + m.toString());
				if (b != null && b.compareTo(ballot) >= 0) {
					ballot = b;
					accepted.add(new PValue(ballot, msg.getsValue()));
				}
				msg.setP2BContent(ballot, m.getThreadId());
				config.logger.info("Sending P2A msg:" + msg.toString());
				nc.sendMessageToServer(b.getlId(), msg);
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
