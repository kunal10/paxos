package ut.distcomp.paxos;

import java.io.Serializable;
import java.util.Objects;

public class SValue implements Serializable{
	public SValue(int slot, Command command) {
		super();
		this.slot = slot;
		this.command = command;
	}
	
	public SValue(SValue other) {
		this(other.getSlot(), other.getCommand());
	}

	public int getSlot() {
		return slot;
	}
	
	public Command getCommand() {
		return command;
	}
	
	public String toString() {
	    StringBuilder result = new StringBuilder();
	    result.append("\nSlot:" + slot);
	    result.append("\nCommand:" + command.toString());
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
		SValue other = (SValue) obj;
		return (slot == other.getSlot() &&
				command.equals(other.getCommand()));
	}
	@Override
	public int hashCode(){
	    return Objects.hash(this.slot, this.command);
	}
	
	private int slot;
	private Command command;
}