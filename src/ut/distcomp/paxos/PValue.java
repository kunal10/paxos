package ut.distcomp.paxos;

import java.io.Serializable;
import java.util.Objects;

public class PValue implements Serializable, Comparable<PValue> {
	public PValue(Ballot ballot, SValue sValue) {
		super();
		this.ballot = new Ballot(ballot);
		this.sValue = new SValue(sValue);
	}
	
	public PValue(PValue other) {
		this(other.getBallot(), other.getsValue());
	}
	
	public Ballot getBallot() {
		return ballot;
	}
	
	public SValue getsValue() {
		return sValue;
	}
	
	public String toString() {
	    StringBuilder result = new StringBuilder();
	    result.append("\nBallot:" + ballot.toString());
	    result.append("\nSValue:" + sValue.toString());
	    return result.toString();
	}
	
	// Compares this PValue to other PValue on the basis of their ballots.
	@Override
	public int compareTo(PValue other) {
		return this.getBallot().compareTo(other.getBallot());
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == this) { 
			return true; 
		} 
		if (obj == null || obj.getClass() != this.getClass()) {
			return false; 
		}
		PValue other = (PValue) obj;
		return (ballot.equals(other.getBallot()) &&
				sValue.equals(other.getsValue()));
	}
	@Override
	public int hashCode(){
	    return Objects.hash(this.ballot, this.sValue);
	}
	
	private Ballot ballot;
	private SValue sValue;
	
	private static final long serialVersionUID = 1L;
}