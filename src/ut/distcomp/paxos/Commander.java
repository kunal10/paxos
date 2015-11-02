package ut.distcomp.paxos;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Message.NodeType;

// NOTE : This implementation relies on the fact that at any point only 1 
// commander is spawned by any leader. So we can use the leaderId as the 
// commanderId. This is not true in general !!
public class Commander implements Runnable {
	public Commander(Config config, NetController nc, int commanderId,
			PValue pValue) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = nc.getCommanderQueue(commanderId);
		this.commanderId = commanderId;
		this.pValue = new PValue(pValue);
	}
	
	// Interacts with other replicas to recover the lost state.
	public void recover() {
		// TODO : Implement this.
	}
	
	public void run() {
		Set<Integer> received = new HashSet<Integer>();
		// Send P2A message to all acceptors.
		sendP2AToAcceptors();
		while (true) {
			Message m = null;
			try {
				m = queue.take();
			} catch (InterruptedException e) {
				config.logger.severe(e.getMessage());
				continue;
			}
			Ballot b = pValue.getBallot();
			Ballot b1 = m.getBallot();
			if (b1 == null) {
				config.logger.severe("Received msg without any ballot:" +
						m.toString());
				continue;
			}
			switch(m.getMsgType()) {
			case P2B:
				if (b1.equals(b)) {
					received.add(m.getSrc());
					// If received majority 
					if (received.size() >= (config.numOfServers/2 + 1)) {
						// Send the decision to all replicas.
						sendDecisionToReplicas();
						// TODO(klad) : Check if this can cause any issues.
						return;
					}
				} else {
					// Send PreEmpted message.
					Message msg = new Message(commanderId, b.getlId());
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
	
	private void sendP2AToAcceptors() {
		config.logger.info("Sending P2A msg to all Acceptors for pvalue:" +
				pValue.toString());
		Message msg = null;
		for (int acceptorId = 0; acceptorId < config.numOfServers;
				acceptorId++) {
			msg = new Message(commanderId, acceptorId);
			msg.setP2AContent(pValue, commanderId);
			nc.sendMessageToServer(acceptorId, msg);
		}
	}
	
	private void sendDecisionToReplicas() {
		config.logger.info("Sending Decision to all replicas:" +
				pValue.toString());
		Message msg = null;
		for (int replicaId = 0; replicaId < config.numOfServers;
				replicaId++) {
			msg = new Message(commanderId, replicaId);
			msg.setDecisionContent(pValue.getsValue());
			nc.sendMessageToServer(replicaId, msg);
		}
	}
	
	private int commanderId;
	private PValue pValue;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
}
