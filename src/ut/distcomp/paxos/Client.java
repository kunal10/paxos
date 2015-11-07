package ut.distcomp.paxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
		this.chatLog = new Command[ChatLogSize];
		this.clientId = clientId;
		this.numOfServers = config.numServers;
		this.clientQueue = new LinkedBlockingQueue<>();
		this.config = config;
		this.nc = new NetController(config, numOfServers, clientQueue);
		this.outstandingRequests = new HashSet<>();
		startReceiveThread();
	}

	/**
	 * Start the receive thread.
	 */
	private void startReceiveThread() {
		receiveThread = new Thread(new ReceiveHandler());
		receiveThread.start();
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
		for (int i = 0; i < numOfServers; i++) {
			Message msg = new Message(clientId, i);
			msg.setRequestContent(new Command(clientId, currentCommandId,
					CommandType.SEND_MSG, m));
			if (nc.sendMessageToServer(i, msg)) {
				config.logger.info("Succesfully sent to " + i
						+ "\nMessage Sent : " + "\n" + msg.toString());
			} else {
				config.logger.info("Unsuccessful send to " + i
						+ "\nMessage Sent : " + "\n" + msg.toString());
			}
		}
	}

	/**
	 * Return true if there are no more requests to be serviced for this client.
	 * 
	 * @return
	 */
	public boolean areAllCommandsExecuted() {
		return outstandingRequests.size() == 0;
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
					SValue sValue = m.getsValue();
					chatLog[sValue.getSlot()] = sValue.getCommand();
					config.logger.info("Set " + sValue.getCommand().toString()
							+ " at index " + sValue.getSlot());
					// Remove an outstanding request if you have received.
					if (sValue.getCommand().getClientId() == clientId) {
						int commandIdToBeRemoved = sValue.getCommand()
								.getCommandId();
						if (outstandingRequests
								.contains(commandIdToBeRemoved)) {
							outstandingRequests.remove(
									Integer.valueOf(commandIdToBeRemoved));
							config.logger.info("Removed " + commandIdToBeRemoved
									+ " on receipt of message " + m.toString());
						}
					}
				} catch (InterruptedException e) {
					config.logger.log(Level.SEVERE, "Client " + clientId
							+ " interrupted while waiting for message");
				}
			}
		}
	}

	/**
	 * Gets the client's chat log
	 */
	public List<String> getChatLog() {
		List<Command> addedCommands = new ArrayList<>();
		for (int i = 0; i < ChatLogSize; i++) {
			Command c = chatLog[i];
			if (c != null && !addedCommands.contains(c)) {
				addedCommands.add(c);
				for (Command command : addedCommands) {
					config.logger.info(command.toString()+"\n----\n");
				}
			}
		}
		return getPrintMessages(addedCommands);
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
		return index + " " + c.getClientId() + ":" + c.getInput();
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
	 * TODO: Change it to some different structure.
	 */
	private Command[] chatLog;
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
	private HashSet<Integer> outstandingRequests;

	/**
	 * Reference to the receive thread
	 */
	private Thread receiveThread;

	private static final int ChatLogSize = 100;
}