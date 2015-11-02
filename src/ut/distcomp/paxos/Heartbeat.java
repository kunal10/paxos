package ut.distcomp.paxos;

import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Heartbeat implements Runnable {

	public Heartbeat(Config config, NetController nc, int serverId) {
		super();
		this.config = config;
		this.nc = nc;
		this.heartbeatQueue = nc.getHeartbeatQueue();
		this.serverId = serverId;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
	final private Config config;
	final private NetController nc;
	private BlockingQueue<Message> heartbeatQueue;
	private int currentPlId;
	final private int serverId;
}
