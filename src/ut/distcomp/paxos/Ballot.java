package ut.distcomp.paxos;

import java.util.Objects;

public class Ballot implements Comparable<Ballot> {
	public Ballot(int value, int lId) {
		super();
		this.value = value;
		this.lId = lId;
	}
	
	public Ballot(Ballot other) {
		this(other.getValue(), other.getlId());
	}
	
	public int getValue() {
		return value;
	}
	
	public int getlId() {
		return lId;
	}
	
	public String toString() {
	    StringBuilder result = new StringBuilder();
	    result.append("\nValue:" + value);
	    result.append("\nlId:" + lId);
	    return result.toString();
	}
	
	@Override
	public int compareTo(Ballot other) {
		final int SMALLER = -1;
	    final int EQUAL = 0;
	    final int GREATER = 1;
	    
		if (this == other) {
			return EQUAL;
		}
		
		if (this.value > other.getValue()) {
			return GREATER;
		} else if (this.value < other.getValue()) {
			return SMALLER;
		} else {
			if (this.lId > other.getlId()) {
				return GREATER;
			} else if (this.lId < other.getlId()) {
				return SMALLER;
			} 
		}
		return EQUAL;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) { 
			return true; 
		} 
		if (obj == null || obj.getClass() != this.getClass()) {
			return false; 
		}
		Ballot other = (Ballot) obj;
		return (compareTo(other) == 0);
	}
	
	@Override
	public int hashCode(){
	    return Objects.hash(this.value, this.lId);
	}
	
	// Value of this ballot.
	private int value;
	// Id of the leader who proposed this Ballot.
	private int lId;
}