package ut.distcomp.paxos;

import java.util.TimerTask;

class BooleanRef {
	public boolean getValue() {
		return value;
	}

	public void setValue(boolean value) {
		this.value = value;
	}

	public BooleanRef(boolean b){
		value = b;
	}
	
	private boolean value;
};

public class TimeoutUtil extends TimerTask{
	
	public TimeoutUtil(BooleanRef timeOut) {
		super();
		this.timeOut = timeOut;
	}

	BooleanRef timeOut;

	@Override
	public void run() {
		timeOut.setValue(true);
	}
}
