package ut.distcomp.paxos;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Message.NodeType;

public class Scout extends Thread {
	public Scout(Config config, NetController nc, int leaderId, int scoutId,
			Ballot b) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = nc.getScoutQueue(scoutId);
		this.leaderId = leaderId;
		this.scoutId = scoutId;
		this.b = new Ballot(b);
	}

	public void run() {
		Set<Integer> received = new HashSet<Integer>();
		Set<PValue> accepted = new HashSet<PValue>();
		// Send P1A message to all acceptors.
		sendP1AToAcceptors();
		while (true) {
			Message m = null;
			try {
				m = queue.take();
			} catch (InterruptedException e) {
				config.logger.severe(e.getMessage());
				continue;
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
					msg.setPreEmptedContent(NodeType.SCOUT, b1);
					config.logger.info(
							"Sending PreEmpted to leader:" + msg.toString());
					nc.sendMessageToServer(leaderId, msg);
					return;
				}
				break;
			default:
				config.logger.severe("Received Unexpected Msg" + m.toString());
				break;
			}
		}
	}

	private void sendP1AToAcceptors() {
		config.logger.info(
				"Sending P1A msg to all Acceptors for ballot:" + b.toString());
		Message msg = null;
		for (int acceptorId = 0; acceptorId < config.numServers; acceptorId++) {
			msg = new Message(leaderId, acceptorId);
			msg.setP1AContent(b, scoutId);
			nc.sendMessageToServer(acceptorId, msg);
		}
	}

	private int leaderId;
	private int scoutId;
	private Ballot b;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
}
