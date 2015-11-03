package ut.distcomp.paxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

public class Replica implements Runnable {
	public Replica(Config config, NetController nc, int replicaId, BlockingQueue<Message> q) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = q;
		this.replicaId = replicaId;
		this.slotNum = 0;
		this.state = new ArrayList<String>();
		this.proposals = new HashSet<SValue>();
		this.decisions = new HashSet<SValue>();
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
			} catch (Exception e) {
				config.logger.severe(e.getMessage());
				continue;
			}
			switch (m.getMsgType()) {
			case REQUEST:
				config.logger.info("Received Request:" + m.toString());
				Command command = m.getCommand();
				if (command == null) {
					config.logger.severe("Received invalid request: " +
							m.toString());
					break;
				}
				propose(m.getCommand());
				break;
			case DECISION:
				config.logger.info("Received Decision:" + m.toString());
				// Add decision to decisions.
				SValue decision = m.getsValue();
				if (decision == null) {
					config.logger.severe("Received decision without svalue: " +
							m.toString());
					break;
				}
				decisions.add(decision);
				// Find decision for current slot.
				SValue p1 = getDecisionForSlot(slotNum);
				while (p1 != null) {
					config.logger.info("Found decision for slot:" + 
							p1.getSlot());
					Command p1c = p1.getCommand();
					// If you had proposed a command for current slot and it was
					// not decided then re-propose it.
					SValue p2 = getProposalForSlot(slotNum);
					if (p2 != null) {
						Command p2c = p2.getCommand();
						if (p2c != null && !p1c.equals(p2c)) {
							config.logger.info("Re-proposing previous proposal:"
									+ p2c.toString());
							propose(p2c);
						}
					}
					// Perform command decided for current slot.
					perform(p1c);
					// Find decision for current slot.
					p1 = getDecisionForSlot(slotNum);
				}
				break;
			default:
				config.logger.severe("Received Unexpected Msg" + m.toString());
				break;
			}
		}
	}

	private void propose(Command c) {
		int s = getEarliestDecidedSlot(c);
		// Propose only if the command has not been decided.
		if (s != -1) {
			return;
		}
		int s1 = getNextFreeSlot();
		SValue proposal = new SValue(s1, c);
		proposals.add(proposal);
		// Send proposal to all leaders.
		for (int leaderId = 0; leaderId < config.numOfServers; leaderId++) {
			Message msg = new Message(replicaId, leaderId);
			msg.setProposeContent(s1, c);
			config.logger.info("Sending Propose msg:" + msg.toString());
			// TODO(asvenk) : What is the appropriate method to be used here.
			nc.sendMessageToServer(leaderId, msg);
		}
	}

	private void perform(Command c) {
		int s = getEarliestDecidedSlot(c);
		// If c has already been performed then increment the slotNum and exit.
		if (s >= 0 && s < slotNum) {
			slotNum++;
			return;
		}
		state.add(c.getInput());
		slotNum++;
		Message msg = new Message(replicaId, c.getClientId());
		msg.setResponseContent(c, state);
		nc.sendMessageToClient(c.getClientId(), msg);
	}
	
	// Returns smallest slot number for which given command was decided.
	// Returns -1 it the command has not been decided.
	private int getEarliestDecidedSlot(Command c) {
		if (c == null) { 
			return -1; 
		}
		for (SValue decision : decisions) {
			Command decided = decision.getCommand();
			if (decided.equals(c)) {
				return decision.getSlot();
			}
		}
		return -1;
	}
	
	// Returns SValue for slot if its decided, null otherwise.
	private SValue getDecisionForSlot(int slot) {
		for (SValue sv : decisions) {
			if (sv.getSlot() == slot) {
				return sv;
			}
		}
		return null;
	}
	
	// Returns proposed SValue for slot if it exists, null otherwise.
	private SValue getProposalForSlot(int slot) {
		for (SValue sv : proposals) {
			if (sv.getSlot() == slot) {
				return sv;
			}
		}
		return null;
	}
	
	// Returns the lowest slot number which is not present in the set of 
	// proposals union decisions.
	private int getNextFreeSlot() {
		ArrayList<Integer> slots = new ArrayList<Integer>();
		for (SValue sv : proposals) {
			slots.add(sv.getSlot());
		}
		for (SValue sv : decisions) {
			slots.add(sv.getSlot());
		}
		Collections.sort(slots);
		// Iterate and find lowest available slot.
		int nextSlot = 0; 
		for (Integer slot : slots) {
			if (slot > nextSlot) {
				break;
			}
			if (slot == nextSlot) {
				nextSlot++;
			}
		}
		return nextSlot;
	}

	private int slotNum;
	private int replicaId;
	private List<String> state;
	private Set<SValue> proposals;
	private Set<SValue> decisions;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
}
