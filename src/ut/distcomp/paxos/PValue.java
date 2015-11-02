package ut.distcomp.paxos;

import java.util.Objects;

public class PValue {
	public PValue(Ballot ballot, SValue sValue) {
		super();
		this.ballot = ballot;
		this.sValue = sValue;
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
}