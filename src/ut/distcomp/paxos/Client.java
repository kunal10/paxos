package ut.distcomp.paxos;

import java.util.ArrayList;
import java.util.Collections;
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
		this.chatLog = new ArrayList<>();
		this.clientId = clientId;
		this.numOfServers = config.numOfServers;
		this.clientQueue = new LinkedBlockingQueue<>();
		this.config = config;
		this.nc = new NetController(config, numOfServers, clientQueue);
		this.outstandingRequests = Collections.synchronizedList(
				new ArrayList<Integer>());
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
	 * @param serverId
	 * @param m
	 * @return
	 */
	public void sendMessageToChatroom(int serverId, String m) {
		for (int i = 0; i < numOfServers; i++) {
			Message msg = new Message(clientId, i);
			int currentCommandId = getNextUniqueCommandNumber();
			msg.setRequestContent(new Command(clientId, 
					currentCommandId, CommandType.SEND_MSG, m));
			outstandingRequests.add(currentCommandId);
			if (nc.sendMessageToServer(i, msg)) {
				config.logger.info("Succesfully sent to " + i + "\nMessage Sent : " 
			+ "\n" + msg.toString());
			} else {
				config.logger.info("Unsuccessful send to " + i + "\nMessage Sent : " 
			+ "\n" + msg.toString());
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
					List<String> listofMessages = m.getOutput();
					// Update chat log if any new messages have arrived.
					if(listofMessages.size() > chatLog.size()){
						chatLog = listofMessages;
						config.logger.info("Changed the state of chat log "
								+ "consuming Message " + m.toString());
					}
					// Remove an outstanding request if you have received.
					if(m.getCommand().getClientId() == clientId){
						int commandIdToBeRemoved = m.getCommand().getCommandId();
						if(outstandingRequests.contains(commandIdToBeRemoved)){
							outstandingRequests.remove(commandIdToBeRemoved);
							config.logger.info("Removed "+commandIdToBeRemoved +
									" on receipt of message "+m.toString());
						}
					}
				} catch (InterruptedException e) {
					config.logger.log(Level.SEVERE, "Client " + clientId + 
							" interrupted while waiting for message");
				}
			}
		}
	}

	/**
	 * Gets the client's chat log
	 */
	public List<String> getChatLog() {
		return chatLog;
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
	private List<String> chatLog;
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
	private List<Integer> outstandingRequests;

	/**
	 * Reference to the receive thread
	 */
	private Thread receiveThread;
}