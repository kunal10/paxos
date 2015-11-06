package ut.distcomp.paxos;



import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Leader extends Thread {
	public Leader(Config config, NetController nc, int leaderId) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = nc.getAcceptorQueue();
		this.leaderId = leaderId;
		this.active = false;
		this.proposals = new HashSet<SValue>();
		this.scouts = new HashMap<Integer, Scout>();
		this.commanders = new HashMap<Integer, Commander>();
		this.ballot = new Ballot(0,leaderId);
	}
	
	public void run() {
		Scout scout = new Scout(config, nc, leaderId, ballot);
	}
	
	private int leaderId;
	private boolean active;
	private Ballot ballot;
	private Set<SValue> proposals;
	private HashMap<Integer, Scout> scouts;
	private HashMap<Integer, Commander> commanders;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
}
