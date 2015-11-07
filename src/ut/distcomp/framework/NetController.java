/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

/**
* The sendMsg method has been modified by Navid Yaghmazadeh to fix a bug regarding to send a message to a reconnected socket.
*/

package ut.distcomp.framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;

import ut.distcomp.paxos.Message;

/**
 * Public interface for managing network connections.
 * You should only need to use this and the Config class.
 * @author ilevy
 *
 */
public class NetController {
	private final Config config;
	private final List<IncomingSock> inSockets;
	private final OutgoingSock[] outSockets;
	private final ListenServer listener;
	private final int numOfServers;
	/*
	 * Queue used by leader thread to retrieve its messages.
	 */
	private BlockingQueue<Message> leaderQueue;
	/*
	 * Queue used by the replica to retrieve its messages.
	 */
	private BlockingQueue<Message> replicaQueue;
	/*
	 * Queue used by the acceptor to retrieve its messages.
	 */
	private BlockingQueue<Message> acceptorQueue;
	/*
	 * Map of a commander to the queue used by it to retrieve its messages. 
	 */
	private HashMap<Integer,BlockingQueue<Message>> commanderQueues;
	/*
	 * Map of a scout to the queue used by it to retrieve its messages. 
	 */
	private HashMap<Integer,BlockingQueue<Message>> scoutQueues;
	
	private BlockingQueue<Message> heartbeatQueue;
	
	private BlockingQueue<Message> clientQueue;
	
	public NetController(Config config, 
			BlockingQueue<Message> leaderQueue, 
			BlockingQueue<Message> replicaQueue, 
			BlockingQueue<Message> acceptorQueue, 
			HashMap<Integer, BlockingQueue<Message>> commanderQueue, 
			HashMap<Integer, BlockingQueue<Message>> scoutQueue,
			BlockingQueue<Message> heartbeatQueue) {
		this.leaderQueue = leaderQueue;
		this.replicaQueue = replicaQueue;
		this.acceptorQueue = acceptorQueue;
		this.commanderQueues = commanderQueue;
		this.scoutQueues = scoutQueue;
		this.heartbeatQueue = heartbeatQueue;
		this.config = config;
		this.numOfServers = config.numServers;
		inSockets = Collections.synchronizedList(new ArrayList<IncomingSock>());
		listener = new ListenServer(config, inSockets,
				leaderQueue,
				replicaQueue,
				acceptorQueue,
				commanderQueue,
				scoutQueue,
				heartbeatQueue,
				null);
		outSockets = new OutgoingSock[config.numProcesses];
		listener.start();
	}
	
	public NetController(Config config,
			int numOfServers,
			BlockingQueue<Message> clientQueue){
		this.clientQueue = clientQueue;
		this.config = config;
		this.numOfServers = numOfServers;
		inSockets = Collections.synchronizedList(new ArrayList<IncomingSock>());
		listener = new ListenServer(config, inSockets, leaderQueue,
				null,
				null,
				null,
				null,
				null,
				clientQueue);
		outSockets = new OutgoingSock[config.numProcesses];
		listener.start();
	}
	
	public Config getConfig() {
		return config;
	}

	public int getNumOfServers() {
		return numOfServers;
	}

	public BlockingQueue<Message> getLeaderQueue() {
		return leaderQueue;
	}

	public BlockingQueue<Message> getReplicaQueue() {
		return replicaQueue;
	}

	public BlockingQueue<Message> getAcceptorQueue() {
		return acceptorQueue;
	}

	public BlockingQueue<Message> getCommanderQueue(int i) {
		return commanderQueues.get(i);
	}
	
	public HashMap<Integer, BlockingQueue<Message>> getCommanderQueues() {
		return commanderQueues;
	}

	public BlockingQueue<Message> getScoutQueue(int i) {
		return scoutQueues.get(i);
	}
	
	public HashMap<Integer, BlockingQueue<Message>> getScoutQueues() {
		return scoutQueues;
	}

	public BlockingQueue<Message> getHeartbeatQueue() {
		return heartbeatQueue;
	}

	public BlockingQueue<Message> getClientQueue() {
		return clientQueue;
	}

	// Establish outgoing connection to a process
	private synchronized void initOutgoingConn(int proc) throws IOException {
		if (outSockets[proc] != null)
			throw new IllegalStateException("proc " + proc + " not null");
		
		outSockets[proc] = new OutgoingSock(new Socket(config.addresses[proc], 
				config.ports[proc]));
		config.logger.info(String.format("Server %d: Socket to %d established", 
				config.procNum, proc));
	}
	
	/**
	 * Send the message to the specified server ID.
	 * @param serverId
	 * @param msg
	 * @return
	 */
	public boolean sendMessageToServer(int serverId, Message msg){
		return sendMsg(serverId, msg);
	}
	
	/**
	 * Send the message to the specified client ID.
	 * @param clientId
	 * @param msg
	 * @return
	 */
	public boolean sendMessageToClient(int clientId, Message msg){
		config.logger.info("Sending message to a client : "+clientId);
		int clientProcessId = clientId + numOfServers;
		config.logger.info("using client process ID : "+clientProcessId);
		config.logger.info("Sending message to client : "+msg.toString());
		return sendMsg(clientProcessId, msg);
	}
	
	/**
	 * Send a msg to another process.  This will establish a socket if one is not created yet.
	 * Will fail if recipient has not set up their own NetController (and its associated serverSocket)
	 * @param process int specified in the config file - 0 based
	 * @param msg Do not use the "&" character.  This is hardcoded as a message separator. 
	 *            Sends as ASCII.  Include the sending server ID in the message
	 * @return bool indicating success
	 */
	public synchronized boolean sendMsg(int process, Message msg) {
		try {
			if (outSockets[process] == null)
				initOutgoingConn(process);
			outSockets[process].sendMsg(msg);
		} catch (IOException e) { 
			if (outSockets[process] != null) {
				outSockets[process].cleanShutdown();
				outSockets[process] = null;
				try{
					initOutgoingConn(process);
                        		outSockets[process].sendMsg(msg);	
				} catch(IOException e1){
					if (outSockets[process] != null) {
						outSockets[process].cleanShutdown();
	                	outSockets[process] = null;
					}
					config.logger.info(String.format("Server %d: Msg to %d "
							+ "failed.", config.procNum, process));
        		    config.logger.log(Level.FINE, String.format("Server %d: "
        		    		+ "Socket to %d error", config.procNum, process), 
        		    		e);
                    return false;
				}
				return true;
			}
			config.logger.info(String.format("Server %d: Msg to %d failed.", 
				config.procNum, process));
			config.logger.log(Level.FINE, String.format("Server %d: Socket to"
					+ " %d error", config.procNum, process), e);
			return false;
		}
		return true;
	}
	
	/**
	 * Return a list of msgs received on established incoming sockets
	 * @return list of messages sorted by socket, in FIFO order. *not sorted by
	 * time received*
	 */
	public synchronized List<String> getReceivedMsgs() {
		List<String> objs = new ArrayList<String>();
		synchronized(inSockets) {
			ListIterator<IncomingSock> iter  = inSockets.listIterator();
			while (iter.hasNext()) {
				IncomingSock curSock = iter.next();
				try {
					
				} catch (Exception e) {
					config.logger.log(Level.INFO, 
							"Server " + config.procNum + 
							" received bad data on a socket", e);
					curSock.cleanShutdown();
					iter.remove();
				}
			}
		}
		
		return objs;
	}
	/**
	 * Shuts down threads and sockets.
	 */
	public synchronized void shutdown() {
		listener.cleanShutdown();
        if(inSockets != null) {
		    for (IncomingSock sock : inSockets)
			    if(sock != null)
                    sock.cleanShutdown();
        }
		if(outSockets != null) {
            for (OutgoingSock sock : outSockets)
			    if(sock != null)
                    sock.cleanShutdown();
        }
		
	}
	
	public static void main(String[] args){
		Config p1 = null;
		Config p2 = null;
		int numOfServers = 2;
		int numOfClients = 2;
		try {
			p1 = new Config(0, numOfServers, numOfClients, "config_p0.txt");
			p2 = new Config(1, numOfServers, numOfClients, "config_p1.txt");
			
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
		} catch (IOException e) {
			//e.printStackTrace();
		}
		
		BlockingQueue<Message> leader = new LinkedBlockingQueue<Message>();
		HashMap<Integer, BlockingQueue<Message>> commander = new HashMap<>();
		HashMap<Integer, BlockingQueue<Message>> scout = new HashMap<>();
		BlockingQueue<Message> acceptor = new LinkedBlockingQueue<Message>();;
		BlockingQueue<Message> replica = new LinkedBlockingQueue<Message>();
		BlockingQueue<Message> heartbeat = new LinkedBlockingQueue<Message>();
		
		NetController p1_con = new NetController(p1, leader, 
				replica, acceptor, commander, scout, heartbeat);
		NetController p2_con = new NetController(p2, leader, 
				replica, acceptor, commander, scout, heartbeat);
		Message m = new Message(0,1);
		p1_con.sendMsg(1, m);

	}

}
