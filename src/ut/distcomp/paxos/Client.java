package ut.distcomp.paxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;
import ut.distcomp.paxos.Command.CommandType;

/**
 * Client which attempts to add a message to the chat log. Should broadcast a
 * request to all replicas. Update its local chat log from responses from all
 * servers. Should add a unique identifier to all the messages it sends to the
 * replicas.
 * 
 */
public class Client {

	public Client(int clientId, Config config) {
		super();
		this.chatLog = new TreeMap<Integer, Command>();
		this.commandId = 0;
		this.clientId = clientId;
		this.numOfServers = config.numServers;
		this.clientQueue = new LinkedBlockingQueue<>();
		this.config = config;
		this.nc = new NetController(config, numOfServers, clientQueue);
		this.outstandingRequests = Collections.synchronizedSet(new HashSet<>());
		this.receiveThread = null;
		this.timer = new Timer();
		this.reproposeTimers = new HashMap<>();
		startReceiveThread();
	}

	/**
	 * Start the receive thread.
	 */
	private void startReceiveThread() {
		receiveThread = new Thread(new ReceiveHandler());
		receiveThread.start();
	}

	public void CrashClient() {
		nc.shutdown();
		for (ReproposeCommand rc : reproposeTimers.values()) {
			rc.cancel();
		}
		timer.cancel();
		if (receiveThread != null) {
			receiveThread.stop();
		}
	}

	/**
	 * Interface used by the master to send message to a chatroom. The client
	 * broadcasts the message to all the server replicas.
	 * 
	 * @param m
	 * @return
	 */
	public void sendMessageToChatroom(String m) {
		int currentCommandId = getNextUniqueCommandNumber();
		outstandingRequests.add(currentCommandId);
		Command c = new Command(clientId, currentCommandId,
				CommandType.SEND_MSG, m);
		broadcastCommand(c);
		registerReproposeTimer(c);
	}

	private void registerReproposeTimer(Command c) {
		ReproposeCommand rc = new ReproposeCommand(c);
		reproposeTimers.put(c.getCommandId(), rc);
		timer.schedule(rc, Config.ReproposeTimeout);
	}

	public void broadcastCommand(Command c) {
		for (int i = 0; i < numOfServers; i++) {
			Message msg = new Message(clientId, i);
			msg.setRequestContent(c);
			if (nc.sendMessageToServer(i, msg)) {
				config.logger.info("Succesfully sent to " + i
						+ "\nMessage Sent : " + "\n" + msg.toString());
			} else {
				config.logger.info("Unsuccessful send to " + i
						+ "\nMessage Sent : " + "\n" + msg.toString());
			}
		}
	}

	class ReproposeCommand extends TimerTask {
		Command command;

		public ReproposeCommand(Command c) {
			this.command = c;
		}

		@Override
		public void run() {
			config.logger
					.info("Client reproposing command " + command.toString());
			broadcastCommand(command);
		}
	}

	/**
	 * Return true if there are no more requests to be serviced for this client.
	 * 
	 * @return
	 */
	public boolean allCommandsExecuted() {
		return outstandingRequests.size() == 0;
	}

	/**
	 * Gets the client's chat log
	 */
	public List<String> getChatLog() {
		List<Command> addedCommands = new ArrayList<>();
		int curSlot = 0;
		synchronized (chatLog) {
			for (Integer slot : chatLog.keySet()) {
				if (slot > curSlot) {
					break;
				}
				Command c = chatLog.get(slot);
				if (c == null) {
					break;
				}
				if (!addedCommands.contains(c)) {
					addedCommands.add(c);
				}
				curSlot++;
			}
		}
		for (Command command : addedCommands) {
			config.logger.info(command.toString() + "\n----\n");
		}
		return getPrintMessages(addedCommands);
	}

	/**
	 * Thread for receiving all messages from all the servers in chatroom.
	 */
	class ReceiveHandler implements Runnable {
		@Override
		public void run() {
			while (true) {
				try {
					Message m = clientQueue.take();
					decideSlot(m.getsValue());
				} catch (Exception e) {
					config.logger.log(Level.SEVERE, "\nClient " + clientId
							+ " interrupted while waiting for message");
					return;
				}
			}
		}
	}

	private void decideSlot(SValue sValue) {
		if (sValue == null) {
			config.logger.severe("\nReceived NULL sValue in decideSlot");
			return;
		}
		synchronized (chatLog) {
			chatLog.put(sValue.getSlot(), sValue.getCommand());
		}
		config.logger.info("\n\nReceived " + sValue.getCommand().toString()
				+ "\tat index " + sValue.getSlot());
		removeOutstandingRequest(sValue.getCommand());
	}

	// Removes the request from the set of out standing requests if it has not
	// already been removed.
	private void removeOutstandingRequest(Command c) {
		if (c == null) {
			config.logger
					.severe("Null command passed to removeOutstandingRequest");
			return;
		}
		if (c.getClientId() == clientId) {
			int cId = c.getCommandId();
			if (outstandingRequests.contains(cId)) {
				outstandingRequests.remove(Integer.valueOf(cId));
				config.logger.info("\nRemoved " + cId);
			}
			if(reproposeTimers.containsKey(cId)){
				// Cancel the timer for this command
				reproposeTimers.get(cId).cancel();
				config.logger.info("\nRemoved Timer :" + cId);
			}
		}
		printOutstandingRequests();
	}

	private void printOutstandingRequests() {
		config.logger.info("\nOutstanding Requests:");
		synchronized (outstandingRequests) {
			for (Integer request : outstandingRequests) {
				config.logger.info("\t" + request);
			}
		}
	}

	private List<String> getPrintMessages(List<Command> addedCommands) {
		List<String> responseToMaster = new ArrayList<>();
		for (int i = 0; i < addedCommands.size(); i++) {
			Command c = addedCommands.get(i);
			if (c != null) {
				responseToMaster.add(formatCommand(i, c));
			}
		}
		return responseToMaster;
	}

	private String formatCommand(int index, Command c) {
		return index + " " + c.getClientId() + ": " + c.getInput();
	}

	/**
	 * Used to generate unique command numbers for each command issued by the
	 * client
	 * 
	 * @return
	 */
	private int getNextUniqueCommandNumber() {
		int commandIdToSend = commandId;
		++commandId;
		return commandIdToSend;
	}

	/**
	 * Chat log of this client received from the servers.
	 */
	private SortedMap<Integer, Command> chatLog;
	/**
	 * Command ID to be used while sending to a
	 */
	private int commandId;
	/**
	 * ID of this client.
	 */
	final private int clientId;
	/**
	 * Total num of servers in the system.
	 */
	final private int numOfServers;
	/**
	 * Communication framework used by the client.
	 */
	final private NetController nc;
	/**
	 * Config for this client.
	 */
	final private Config config;
	/**
	 * The queue on which all the client messages are received.
	 */
	private BlockingQueue<Message> clientQueue;
	/**
	 * keeps the list of all the outstanding requests to the server which is not
	 * added yet.
	 */
	private Set<Integer> outstandingRequests;

	/**
	 * 
	 */
	private final HashMap<Integer, ReproposeCommand> reproposeTimers;

	private Timer timer;
	/**
	 * Reference to the receive thread
	 */
	private Thread receiveThread;
}