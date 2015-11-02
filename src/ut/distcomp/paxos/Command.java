package ut.distcomp.paxos;

public class Command {
	public enum CommandType {
		SEND_MSG,
	};

	public Command(int clientId, int commandId, CommandType commandType, 
			String input) {
		super();
		this.clientId = clientId;
		this.commandId = commandId;
		this.commandType = commandType;
		this.input = input;
	}
	
	public Command(Command other) {
		this(other.getClientId(), other.getCommandId(), other.getCommandType(), 
				other.getInput());
	}

	public int getClientId() {
		return clientId;
	}

	public int getCommandId() {
		return commandId;
	}

	public CommandType getCommandType() {
		return commandType;
	}

	public String getInput() {
		return input;
	}
	
	public String toString() {
	    StringBuilder result = new StringBuilder();
	    result.append("\nclientId:" + clientId);
	    result.append("\ncommandId:" + commandId);
	    result.append("\ncommandType:" + commandType);
	    result.append("\ninput:" + input);
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
		Command other = (Command) obj;
		return (clientId == other.getClientId() &&
				commandId == other.getCommandId() &&
				commandType == other.getCommandType() &&
				input.equals(other.getInput()));
	}

	private int clientId;
	private int commandId;
	private CommandType commandType;
	private String input;
}
