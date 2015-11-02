package ut.distcomp.paxos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Message implements Serializable {
	public enum NodeType {
		CLIENT, SERVER, REPLICA, LEADER, COMMANDER, SCOUT, ACCEPTOR,
	};
	
	public enum MessageType {
		// MessageType		SourceType		DestinationType
		HEART_BEAT,     	// SERVER       SERVER 
		REQUEST,  			// CLIENT		REPLICA
		RESPONSE, 			// REPLICA		CLIENT
		STATE_REQ, 			// X			REPLICA     :X in {RELICA,ACCEPTOR}
		STATE_RES, 			// REPLICA		X			:X in {RELICA,ACCEPTOR}
		PROPOSE, 			// REPLICA		LEADER
		ADOPTED, 			// SCOUT		LEADER
		P1A, 				// SCOUT		ACCEPTOR
		P2A, 				// COMMANDER	ACCEPTOR
		P1B, 				// ACCEPTOR		SCOUT
		P2B,				// ACCEPTOR		COMMANDER
		DECISION, 			// COMMANDER	REPLICA
		PRE_EMPTED, 		// X			LEADER		:X in {SCOUT,COMMANDER}
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
		this.output = null;
		this.accepted = null;
		this.proposals = null;
		this.decisions = null;
		this.primary = -1;
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
	
	public void setResponseContent(Command c, List<String> l) {
		srcType = NodeType.REPLICA;
		destType = NodeType.CLIENT;
		msgType = MessageType.RESPONSE;
		command = new Command(c);
		output = new ArrayList<String>(l);
	}
	
	public void setStateRequestContent(NodeType nt) {
		if (nt != NodeType.ACCEPTOR && nt != NodeType.REPLICA) {
			// TODO : Add Log(Severe)
			return;
		}
		srcType = nt;
		destType = NodeType.REPLICA;
		msgType = MessageType.STATE_REQ;
	}
	
	public void setStateResponseContent(NodeType nt, Set<SValue> d, 
			Set<SValue> p) {
		if (nt != NodeType.ACCEPTOR && nt != NodeType.REPLICA) {
			// TODO : Add Log(Severe)
			return;
		}
		srcType = NodeType.REPLICA;
		destType = nt;
		msgType = MessageType.STATE_RES;
		decisions = new HashSet<SValue>(d);
		proposals = new HashSet<SValue>(p);
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
	
	public void setP1AContent(Ballot b) {
		srcType = NodeType.SCOUT;
		destType = NodeType.ACCEPTOR;
		msgType = MessageType.P1A;
		ballot = new Ballot(b);
	}
	
	public void setP2AContent(PValue pv) {
		srcType = NodeType.COMMANDER;
		destType = NodeType.ACCEPTOR;
		msgType = MessageType.P2A;
		ballot = new Ballot(pv.getBallot());
		sValue = new SValue(pv.getsValue());
	}
	
	public void setP1BContent(Ballot b, Set<PValue> pvalues) {
		srcType = NodeType.ACCEPTOR;
		destType = NodeType.SCOUT;
		msgType = MessageType.P1B;
		ballot = new Ballot(b);
		accepted = new HashSet<PValue>(pvalues);
	}
	
	public void setP2BContent(Ballot b) {
		srcType = NodeType.ACCEPTOR;
		destType = NodeType.COMMANDER;
		msgType = MessageType.P2B;
		ballot = new Ballot(b);
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

	public List<String> getOutput() {
		return output;
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

	public String toString() {
		StringBuilder result = new StringBuilder();
		result.append("\nSrc: " + src);
		result.append("\nDest: " + dest);
		result.append("\nSrcType: " + srcType.name());
		result.append("\nDestType: " + destType.name());
		// Type specific content fields. 
		if (ballot != null) {
			result.append("\nCommand: " + ballot.toString());
		}
		if (sValue != null) {
			result.append("\nsValue: " + sValue.toString());
		}
		if (command != null) {
			result.append("\nCommand: " + command.toString());
		}
		if (output != null) {
			result.append("\nOutput:");
			for (String elem : output) {
				result.append("\n" + elem);
			}
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
	private Ballot ballot;
	// Present in PROPOSE, DECISION and P2A messages.
	private SValue sValue;
	// Present in REQUEST and RESPONSE messages.
	private Command command;
	// Present in RESPONSE messages.
	private List<String> output;
	// Present in ADOPTED and P1B messages.
	private Set<PValue> accepted;
	// Present in STATE_RES messages.
	private Set<SValue> proposals;
	private Set<SValue> decisions;
	// Present in HeartBeat messages.
	private int primary;

	private static final long serialVersionUID = 1L;
}
