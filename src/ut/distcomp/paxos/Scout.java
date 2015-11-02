package ut.distcomp.paxos;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Message.NodeType;

public class Scout {
	public Scout(Config config, NetController nc, int commanderId, Ballot b) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = nc.getScoutQueue();
		this.commanderId = commanderId;
		this.b = new Ballot(b);
	}
	
	// Interacts with other replicas to recover the lost state.
	public void recover() {
		// TODO : Implement this.
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
				config.logger.severe("Received msg without any ballot:" +
						m.toString());
				continue;
			}
			Message msg = new Message(commanderId, b.getlId());
			switch(m.getMsgType()) {
			case P1B:
				if (b1.equals(b)) {
					accepted.addAll(m.getAccepted());
					received.add(m.getSrc());
					// If received majority.
					if (received.size() >= (config.numOfServers/2 + 1)) {
						// Send the adopted msg to leader.
						msg.setAdoptedContent(b, accepted);
						config.logger.info("Sending Adopted to leader:" + 
								msg.toString());
						nc.sendMessageToServer(b.getlId(), msg);
						// TODO(klad) : Check if this can cause any issues.
						return;
					}
				} else {
					// Send PreEmpted message.
					msg.setPreEmptedContent(NodeType.LEADER, b1);
					config.logger.info("Sending PreEmpted to leader:" + 
							msg.toString());
					nc.sendMessageToServer(b.getlId(), msg);
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
		config.logger.info("Sending P1A msg to all Acceptors for ballot:" +
				b.toString());
		Message msg = null;
		for (int acceptorId = 0; acceptorId < config.numOfServers;
				acceptorId++) {
			msg = new Message(commanderId, acceptorId);
			msg.setP1AContent(b);
			nc.sendMessageToServer(acceptorId, msg);
		}
	}
	
	private int commanderId;
	private Ballot b;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
}
