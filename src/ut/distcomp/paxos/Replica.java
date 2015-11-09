package ut.distcomp.paxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Message.MessageType;
import ut.distcomp.paxos.Message.NodeType;

public class Replica extends Thread {
	public Replica(Config config, NetController nc, int replicaId,
			BlockingQueue<Message> q) {
		super();
		this.config = config;
		this.nc = nc;
		this.queue = q;
		this.replicaId = replicaId;
		this.slotNum = 0;
		this.proposals = new HashSet<SValue>();
		this.decisions = new HashSet<SValue>();
	}

	// Interacts with other replicas to recover the lost state.
	public void recover() {
		config.logger.info("Retriving state for replica");
		for (int i = 0; i < config.numServers; i++) {
			if (i != replicaId) {
				// TODO(asvenk) : Consider having 2 functions:
				// sendStateReq and waitForStateResponse.
				Message m = new Message(replicaId, i);
				m.setStateRequestContent(NodeType.REPLICA);
				// TODO(asvenk) : What if the server for whom you are waiting
				// fails. Although no new servers are killed, previously time-
				// bombed leader can die. This will block in that case.
				if (nc.sendMessageToServer(i, m)) {
					try {
						Message recoverMsg = queue.take();
						while (recoverMsg
								.getMsgType() != MessageType.STATE_RES) {
							recoverMsg = queue.take();
						}
						// TODO(asvenk) : Can this ever happen ??
						// If not then clean up.
						if (recoverMsg.getMsgType() == MessageType.STATE_RES) {
							decisions = recoverMsg.getDecisions();
							proposals = recoverMsg.getProposals();
							break;
						} else {
							config.logger.info("Received non state response :"
									+ "" + recoverMsg.toString());
						}
					} catch (Exception e) {
						config.logger.severe("Interrupted while receiving "
								+ "replica state");
						return;
					}
				}
			}
		}
		sendProposalsToLeaderOnRecovery();
		config.logger.info("Finished recovery for replica");
	}

	// Send all proposals which aren't in decision to the leader.
	private void sendProposalsToLeaderOnRecovery() {
		Set<SValue> difference = new HashSet<>(proposals);
		difference.removeAll(decisions);
		for (SValue sValue : difference) {
			propose(sValue.getCommand());
		}
	}

	public void run() {
		while (true) {
			Message m = null;
			try {
				m = queue.take();
			} catch (InterruptedException e) {
				config.logger.severe(e.getMessage());
				return;
			} catch (Exception e) {
				config.logger.severe(e.getMessage());
				return;
			}
			switch (m.getMsgType()) {
			case STATE_RES:
				config.logger.info("Received State Response:" + m.toString());
				config.logger
						.info("This will be ignored since the first message received"
								+ " is already consumed");
			case STATE_REQ:
				config.logger.info("Received State Request:" + m.toString());
				Message response = new Message(replicaId, m.getSrc());
				response.setStateResponseContent(NodeType.REPLICA, decisions,
						proposals);
				config.logger
						.info("Sending Response msg:" + response.toString());
				nc.sendMessageToServer(m.getSrc(), response);
				break;
			case REQUEST:
				config.logger.info("Received Request:" + m.toString());
				Command command = m.getCommand();
				if (command == null) {
					config.logger.severe(
							"Received invalid request: " + "" + m.toString());
					break;
				}
				propose(m.getCommand());
				break;
			case DECISION:
				config.logger.info("Received Decision:" + m.toString());
				// Add decision to decisions.
				SValue decision = m.getsValue();
				if (decision == null) {
					config.logger.severe("Received decision without svalue: "
							+ "" + m.toString());
					break;
				}
				decisions.add(decision);
				// Find decision for current slot.
				SValue p1 = getDecisionForSlot(slotNum);
				while (p1 != null) {
					config.logger.info(
							"Found decision for slot:" + "" + p1.getSlot());
					Command p1c = p1.getCommand();
					// If you had proposed a command for current slot and it was
					// not decided then re-propose it.
					SValue p2 = getProposalForSlot(slotNum);
					if (p2 != null) {
						Command p2c = p2.getCommand();
						if (p2c != null && !p1c.equals(p2c)) {
							config.logger.info("Re-proposing previous proposal:"
									+ "" + p2c.toString());
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
		// TODO: Just send to your leader.
		// for (int leaderId = 0; leaderId < config.numServers; leaderId++) {
		Message msg = new Message(replicaId, replicaId);
		msg.setProposeContent(s1, c);
		config.logger.info("Sending Propose msg:" + msg.toString());
		nc.sendMessageToServer(replicaId, msg);
		// }
	}

	private void perform(Command c) {
		int s = getEarliestDecidedSlot(c);
		// If c has already been performed then increment the slotNum and exit.
		if (s >= 0 && s < slotNum) {
			slotNum++;
			return;
		}
		slotNum++;
		// Broadcast the message to all clients
		for (int i = 0; i < config.numClients; i++) {
			Message msg = new Message(replicaId, i);
			msg.setResponseContent(new SValue(s, c), c.getInput());
			nc.sendMessageToClient(i, msg);
		}

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
	private Set<SValue> proposals;
	private Set<SValue> decisions;
	private BlockingQueue<Message> queue;
	private NetController nc;
	private Config config;
}
