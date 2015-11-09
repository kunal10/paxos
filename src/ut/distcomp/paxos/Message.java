package ut.distcomp.paxos;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class Message implements Serializable {
	public enum NodeType {
		CLIENT, SERVER, REPLICA, LEADER, COMMANDER, SCOUT, ACCEPTOR,
	};

	public enum MessageType {
		// @formatter:off
		// MessageType	SourceType		DestinationType
		HEART_BEAT, 	// SERVER 		SERVER
		REQUEST, 		// CLIENT 		REPLICA
		RESPONSE, 		// REPLICA 		CLIENT
		STATE_REQ, 		// X 			REPLICA 		:X in {RELICA,ACCEPTOR}
		STATE_RES, 		// REPLICA 		X 				:X in {RELICA,ACCEPTOR}
		PROPOSE, 		// REPLICA 		LEADER
		ADOPTED, 		// SCOUT 		LEADER
		P1A, 			// SCOUT 		ACCEPTOR
		P2A, 			// COMMANDER 	ACCEPTOR
		P1B, 			// ACCEPTOR 	SCOUT
		P2B, 			// ACCEPTOR 	COMMANDER
		DECISION, 		// COMMANDER 	REPLICA
		PRE_EMPTED, 	// X 			LEADER 			:X in {SCOUT,COMMANDER}
		BLOCKED,		// X 			LEADER			:X in {SCOUT,COMMANDER}
		// @formatter:on
	}

	public Message(int src, int dest) {
		super();
		this.src = src;
		this.dest = dest;
		this.srcType = null;
		this.destType = null;
		this.msgType = null;
		this.ballot = null;
		this.sValue = null;
		this.command = null;
		this.accepted = null;
		this.proposals = null;
		this.decisions = null;
		this.primary = -1;
		this.threadId = -1;
	}

	public void setHeartBeatContent(int pId) {
		srcType = NodeType.SERVER;
		destType = NodeType.SERVER;
		msgType = MessageType.HEART_BEAT;
		primary = pId;
	}

	public void setRequestContent(Command c) {
		srcType = NodeType.CLIENT;
		destType = NodeType.REPLICA;
		msgType = MessageType.REQUEST;
		command = new Command(c);
	}

	public void setResponseContent(SValue s, String response) {
		srcType = NodeType.REPLICA;
		destType = NodeType.CLIENT;
		msgType = MessageType.RESPONSE;
		sValue = new SValue(s);
	}

	public void setStateRequestContent(NodeType nt) {
		if (nt != NodeType.ACCEPTOR && nt != NodeType.REPLICA) {
			// TODO : Add Log(Severe)
			return;
		}
		srcType = nt;
		destType = nt;
		msgType = MessageType.STATE_REQ;
	}

	public void setStateResponseContent(NodeType nt, Set<SValue> d,
			Set<SValue> p) {
		if (!(nt == NodeType.REPLICA)) {
			// TODO : Add Log(Severe)
			return;
		}
		srcType = NodeType.REPLICA;
		destType = nt;
		msgType = MessageType.STATE_RES;
		decisions = new HashSet<SValue>(d);
		proposals = new HashSet<SValue>(p);
	}

	public void setStateResponseContent(NodeType nt, Set<PValue> a, Ballot b) {
		if (!(nt == NodeType.ACCEPTOR)) {
			// TODO : Add Log(Severe)
			return;
		}
		srcType = NodeType.ACCEPTOR;
		destType = nt;
		msgType = MessageType.STATE_RES;
		accepted = new HashSet<PValue>(a);
		ballot = new Ballot(b);
	}

	public void setProposeContent(int slot, Command c) {
		srcType = NodeType.REPLICA;
		destType = NodeType.LEADER;
		msgType = MessageType.PROPOSE;
		sValue = new SValue(slot, c);
	}

	public void setAdoptedContent(Ballot b, Set<PValue> pvalues) {
		srcType = NodeType.SCOUT;
		destType = NodeType.LEADER;
		msgType = MessageType.ADOPTED;
		ballot = new Ballot(b);
		accepted = new HashSet<PValue>(pvalues);
	}

	public void setP1AContent(Ballot b, int scoutId) {
		srcType = NodeType.SCOUT;
		destType = NodeType.ACCEPTOR;
		msgType = MessageType.P1A;
		ballot = new Ballot(b);
		threadId = scoutId;
	}

	public void setP2AContent(PValue pv, int commanderId) {
		srcType = NodeType.COMMANDER;
		destType = NodeType.ACCEPTOR;
		msgType = MessageType.P2A;
		ballot = new Ballot(pv.getBallot());
		sValue = new SValue(pv.getsValue());
		threadId = commanderId;
	}

	public void setP1BContent(Ballot b, Set<PValue> pvalues, int scoutId) {
		srcType = NodeType.ACCEPTOR;
		destType = NodeType.SCOUT;
		msgType = MessageType.P1B;
		ballot = new Ballot(b);
		accepted = new HashSet<PValue>(pvalues);
		threadId = scoutId;
	}

	public void setP2BContent(Ballot b, int commanderId) {
		srcType = NodeType.ACCEPTOR;
		destType = NodeType.COMMANDER;
		msgType = MessageType.P2B;
		ballot = new Ballot(b);
		threadId = commanderId;
	}

	public void setDecisionContent(SValue sv) {
		srcType = NodeType.COMMANDER;
		destType = NodeType.REPLICA;
		msgType = MessageType.DECISION;
		sValue = new SValue(sv);
	}

	public void setPreEmptedContent(NodeType nt, Ballot b) {
		if (nt != NodeType.SCOUT && nt != NodeType.COMMANDER) {
			// TODO : Add Log(SEVERE)
			return;
		}
		srcType = nt;
		destType = NodeType.LEADER;
		msgType = MessageType.PRE_EMPTED;
		ballot = new Ballot(b);
	}
	
	public void setBlockedContent(NodeType nt, int threadId) {
		if (nt != NodeType.SCOUT && nt != NodeType.COMMANDER) {
			// TODO : Add Log(SEVERE)
			return;
		}
		srcType = nt;
		destType = NodeType.LEADER;
		msgType = MessageType.BLOCKED;
		this.threadId = threadId;
	}

	public int getSrc() {
		return src;
	}

	public int getDest() {
		return dest;
	}

	public NodeType getSrcType() {
		return srcType;
	}

	public NodeType getDestType() {
		return destType;
	}

	public MessageType getMsgType() {
		return msgType;
	}

	public Ballot getBallot() {
		return ballot;
	}

	public SValue getsValue() {
		return sValue;
	}

	public Command getCommand() {
		return command;
	}

	public Set<PValue> getAccepted() {
		return accepted;
	}

	public Set<SValue> getProposals() {
		return proposals;
	}

	public Set<SValue> getDecisions() {
		return decisions;
	}

	public int getPrimary() {
		return primary;
	}

	public int getThreadId() {
		return threadId;
	}

	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("\n\nSrc: " + src);
		result.append("\nDest: " + dest);
		result.append("\nSrcType: " + srcType.name());
		result.append("\nDestType: " + destType.name());
		result.append("\nMessageType: " + msgType.name());
		// Type specific content fields.
		if (ballot != null) {
			result.append("\nBallot: " + ballot.toString());
		}
		if (sValue != null) {
			result.append("\nsValue: " + sValue.toString());
		}
		if (command != null) {
			result.append("\nCommand: " + command.toString());
		}
		if (accepted != null) {
			result.append("\nAccepted:");
			for (PValue elem : accepted) {
				result.append("\n" + elem.toString());
			}
		}
		if (proposals != null) {
			result.append("\nProposals:");
			for (SValue elem : proposals) {
				result.append("\n" + elem.toString());
			}
		}
		if (decisions != null) {
			result.append("\nDecisions:");
			for (SValue elem : decisions) {
				result.append("\n" + elem.toString());
			}
		}
		result.append("\nThread id :" + threadId);
		result.append("\nPrimary: " + primary);
		return result.toString();
	}

	private int src;
	private int dest;
	private NodeType srcType;
	private NodeType destType;
	private MessageType msgType;

	// Content specific fields.
	// Present in ADOPTED, P1A, P2A, P1B, P2B and PRE_EMPTED messages.
	// Also present in STATE_RES messages when coming from an acceptor
	private Ballot ballot;
	// Present in PROPOSE, DECISION, RESPONSE and P2A messages.
	private SValue sValue;
	// Present in REQUEST and RESPONSE messages.
	private Command command;
	// Present in ADOPTED and P1B messages.
	// Present in STATE_RES messages when coming from an acceptor
	private Set<PValue> accepted;
	// Present in STATE_RES messages.
	private Set<SValue> proposals;
	private Set<SValue> decisions;
	// Present in HeartBeat messages.
	private int primary;
	// Present in P1A, P2A and BLOCKED messages.
	private int threadId;

	private static final long serialVersionUID = 1L;
}
