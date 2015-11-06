package ut.distcomp.paxos;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Leader extends Thread {
	public Leader(Config config, NetController nc,
			BlockingQueue<Boolean> becomePrimary, int leaderId) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = nc.getLeaderQueue();
		this.becomePrimary = becomePrimary;
		this.leaderId = leaderId;
		this.nextScoutId = 0;
		this.nextCommanderId = 0;
		this.active = false;
		this.proposals = new HashSet<SValue>();
		this.scouts = new HashMap<Integer, Scout>();
		this.commanders = new HashMap<Integer, Commander>();
		this.ballot = new Ballot(0, leaderId);
	}

	public void run() {
		// Wait until this leader gets elected as primary.
		try {
			becomePrimary.take();
		} catch (InterruptedException e) {
			config.logger.info(e.getMessage());
		}
		while(true) {
			Message m = null;
			try {
				m = queue.take();
			} catch (InterruptedException e) {
				config.logger.severe(e.getMessage());
				continue;
			}
			switch(m.getMsgType()) {
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
			// TODO(klad) : Handle prempted messages sent when majority cant be
			// achieved by some scout/commander.
			default:
				config.logger.severe("Received Unexpected Msg" + m.toString());
				break;
			}
			
		}
	}

	private void spawnScout() {
		Scout scout = new Scout(config, nc, leaderId, nextScoutId, ballot);
		scouts.put(nextScoutId, scout);
		scout.run();
		nextScoutId++;
	}

	private void spawnCommander(SValue sValue) {
		PValue pValue = new PValue(ballot, sValue);
		Commander commander = new Commander(config, nc, leaderId,
				nextCommanderId, pValue);
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

	private int leaderId;
	private int nextScoutId;
	private int nextCommanderId;
	private boolean active;
	private Ballot ballot;
	private Set<SValue> proposals;
	private HashMap<Integer, Scout> scouts;
	private HashMap<Integer, Commander> commanders;
	private BlockingQueue<Message> queue;
	private BlockingQueue<Boolean> becomePrimary;
	private NetController nc;
	private Config config;
}
