package ut.distcomp.paxos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Leader extends Thread {
	public Leader(Config config, NetController nc, int[] aliveSet,
			AtomicInteger numMsgsToSend, BlockingQueue<Boolean> becomePrimary,
			int leaderId) {
		super();
		this.config = config;
		this.nc = nc;
		this.leaderQueue = nc.getLeaderQueue();
		this.commanderQueues = nc.getCommanderQueues();
		this.scoutQueues = nc.getScoutQueues();
		this.becomePrimary = becomePrimary;
		this.leaderId = leaderId;
		this.nextScoutId = 0;
		this.nextCommanderId = 0;
		this.active = false;
		this.blocked = false;
		this.proposals = new HashSet<SValue>();
		this.scouts = new HashMap<Integer, Scout>();
		this.commanders = new HashMap<Integer, Commander>();
		this.ballot = new Ballot(0, leaderId);
		this.aliveSet = aliveSet;
		this.numMsgsToSend = numMsgsToSend;
	}

	// Returns true if it is still possible to receive messages from a majority
	// given that already received message in the set received.
	public static boolean isBlocked(int[] alive, Set<Integer> received,
			int expected) {
		if (received.size() >= expected) {
			return false;
		}
		Set<Integer> canRespond = new HashSet<>(received);
		synchronized (alive) {
			for (int i = 0; i < alive.length; i++) {
				if (alive[i] != -1) {
					canRespond.add(i);
				}
			}
		}
		return (canRespond.size() < expected);
	}

	public void run() {
		// Wait until this leader gets elected as primary.
		try {
			becomePrimary.take();
		} catch (InterruptedException e) {
			config.logger.info(e.getMessage());
		}
		while (true) {
			if (unblocked()) {
				// Re-spawn a scout whenever you are unblocked due to majority
				// of processes coming back up and reset blocked to false.
				spawnScout();
				blocked = false;
			} else if (blocked) {
				// Wait till majority is back up.
				continue;
			}
			Message m = null;
			try {
				m = leaderQueue.take();
			} catch (InterruptedException e) {
				config.logger.severe(e.getMessage());
				continue;
			}
			switch (m.getMsgType()) {
			case PROPOSE:
				SValue proposal = m.getsValue();
				if (!existsProposalForSlot(proposal.getSlot())) {
					proposals.add(proposal);
					if (active) {
						spawnCommander(proposal);
					}
				}
				break;
			case ADOPTED:
				updateProposals(pmax(m.getAccepted()));
				for (SValue sValue : proposals) {
					spawnCommander(sValue);
				}
				active = true;
				break;
			case PRE_EMPTED:
				Ballot b = m.getBallot();
				if (b.compareTo(ballot) == 1) {
					active = false;
					ballot.setValue(b.getValue() + 1);
					spawnScout();
				}
				break;
			case BLOCKED:
				blocked = true;
				break;
			default:
				config.logger.severe("Received Unexpected Msg" + m.toString());
				break;
			}

		}
	}

	public boolean areScoutsCommandersDead() {
		boolean b = true;
		for (Scout s : scouts.values()) {
			boolean isDeadOrNull = (s == null) || !s.isAlive();
			b = b && isDeadOrNull;
		}
		for (Commander c : commanders.values()) {
			boolean isDeadOrNull = (c == null) || !c.isAlive();
			b = b && isDeadOrNull;
		}
		return b;
	}

	private void spawnScout() {
		// Add blocking queue for new scout.
		BlockingQueue<Message> scoutQueue = new LinkedBlockingQueue<>();
		scoutQueues.put(nextScoutId, scoutQueue);
		// Add a new scout scout.
		Scout scout = new Scout(config, nc, aliveSet, numMsgsToSend, leaderId,
				nextScoutId, ballot);
		scouts.put(nextScoutId, scout);
		scout.run();
		nextScoutId++;
	}

	private void spawnCommander(SValue sValue) {
		PValue pValue = new PValue(ballot, sValue);
		// Add blocking queue for new commander.
		BlockingQueue<Message> commanderQueue = new LinkedBlockingQueue<>();
		commanderQueues.put(nextCommanderId, commanderQueue);
		// Add a new commander.
		Commander commander = new Commander(config, nc, aliveSet, numMsgsToSend,
				leaderId, nextCommanderId, pValue);
		commanders.put(nextCommanderId, commander);
		commander.run();
		nextCommanderId++;
	}

	private boolean existsProposalForSlot(int slot) {
		for (SValue proposal : proposals) {
			if (proposal.getSlot() == slot) {
				return true;
			}
		}
		return false;
	}

	private HashMap<Integer, PValue> pmax(Set<PValue> pValues) {
		HashMap<Integer, PValue> pValueMap = new HashMap<Integer, PValue>();
		for (PValue pValue : pValues) {
			SValue sValue = pValue.getsValue();
			int slot = sValue.getSlot();
			Ballot b = pValue.getBallot();
			if (pValueMap.containsKey(slot)) {
				int compare = pValueMap.get(slot).getBallot().compareTo(b);
				if (compare == -1) {
					// Found a pvalue for this slot with higher ballot.
					pValueMap.put(slot, pValue);
				}
			} else {
				pValueMap.put(slot, pValue);
			}
		}
		return pValueMap;
	}

	private void updateProposals(HashMap<Integer, PValue> pValueMap) {
		// Remove proposals for those slots for which pValueMap contains a
		// proposal.
		Set<SValue> remove = new HashSet<SValue>();
		for (SValue proposal : proposals) {
			if (pValueMap.containsKey(proposal.getSlot())) {
				remove.add(proposal);
			}
		}
		proposals.removeAll(remove);
		// Add all proposals from pValueMap.
		for (PValue pValue : pValueMap.values()) {
			proposals.add(pValue.getsValue());
		}
	}

	private void killAllScoutsAndCommander() {
		for (Commander c : commanders.values()) {
			if (c != null) {
				c.stop();
			}
		}
		for (Scout s : scouts.values()) {
			if (s != null) {
				s.stop();
			}
		}
	}

	// Returns true only if the Leader was in a blocked state and detected that
	// a majority of processes are up now. Does not return true if the leader
	// was already unblocked.
	private boolean unblocked() {
		if (blocked) {
			Set<Integer> dummy = new HashSet<>();
			return !isBlocked(aliveSet, dummy, config.numServers / 2 + 1);
		}
		return false;
	}

	private int leaderId;
	private int nextScoutId;
	private int nextCommanderId;
	private boolean active;
	private boolean blocked;
	private Ballot ballot;
	private Set<SValue> proposals;
	private HashMap<Integer, Scout> scouts;
	private HashMap<Integer, Commander> commanders;
	private BlockingQueue<Message> leaderQueue;
	private HashMap<Integer, BlockingQueue<Message>> commanderQueues;
	private HashMap<Integer, BlockingQueue<Message>> scoutQueues;
	private BlockingQueue<Boolean> becomePrimary;
	private NetController nc;
	private Config config;
	private int[] aliveSet;
	AtomicInteger numMsgsToSend;
}
