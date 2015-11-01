package ut.distcomp.paxos;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import ut.distcomp.framework.Config;
import ut.distcomp.framework.NetController;

/**
 * Client which attempts to add a message to the chat log. Should broadcast a
 * request to all replicas. Update its local chat log from responses from all
 * servers. Should add a unique identifier to all the messages it sends to the
 * replicas.
 * 
 */
public class Client {

	/*
	 * TODO: A client should spawn a thread which keeps listening from all the
	 * servers and updates the chat log when it gets a new response.
	 */

	public Client(int clientId, int numOfServers, Config config) {
		super();
		this.chatLog = new ArrayList<>();
		this.clientId = clientId;
		this.numOfServers = numOfServers;
		this.clientQueue = new LinkedBlockingQueue<>();
		this.config = config;
		this.nc = new NetController(config, numOfServers, clientQueue);
		this.outstandingRequests = new ArrayList<>();
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
			Message msg = new Message();
			// TODO: Construct appropriate message here to send to the server.
			// TODO: Add the request to outstanding requests. Use sync.
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
					// TODO: If the length of the response is higher update the
					// state. Check if the state now contains any message in
					// outstanding requests and remove.
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
	private String getNextUniqueCommandNumber() {
		String uniqueId = clientId + "^" + commandId;
		++commandId;
		return uniqueId;
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
	private List<String> outstandingRequests;

	/**
	 * Reference to the receive thread
	 */
	private Thread receiveThread;
}

class ClientCommand implements Serializable {
	public ClientCommand(String message, String commandNumber, int clientNumber) {
		super();
		this.message = message;
		this.commandNumber = commandNumber;
		this.clientNumber = clientNumber;
	}

	String message;
	String commandNumber;
	int clientNumber;
}
