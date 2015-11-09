package ut.distcomp.paxos;

import java.util.TimerTask;

class IntegerWrapper {
	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public IntegerWrapper(int i) {
		value = i;
	}

	private int value;
};

class BooleanWrapper {
	public boolean getValue() {
		return value;
	}

	public void setValue(boolean value) {
		this.value = value;
	}

	public BooleanWrapper(boolean b) {
		value = b;
	}

	private boolean value;
};

public class TimeoutUtil extends TimerTask {

	public TimeoutUtil(BooleanWrapper timeOut) {
		super();
		this.timeOut = timeOut;
	}

	BooleanWrapper timeOut;

	@Override
	public void run() {
		timeOut.setValue(true);
	}
}
