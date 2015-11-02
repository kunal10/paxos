package ut.distcomp.paxos;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

/**
 * Executes functionality of a acceptor. 
 *
 */
public class Acceptor implements Runnable {
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
		// TODO : Implement this.
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
