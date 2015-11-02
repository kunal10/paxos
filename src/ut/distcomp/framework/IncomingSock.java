/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import ut.distcomp.paxos.Message;

public class IncomingSock extends Thread {
	Socket sock;
	ObjectInputStream in;
	private volatile boolean shutdownSet;
	int bytesLastChecked = 0;
	BlockingQueue<Message> leaderQueue;
	BlockingQueue<Message> replicaQueue;
	BlockingQueue<Message> acceptorQueue;
	BlockingQueue<Message> commanderQueue;
	BlockingQueue<Message> scoutQueue;
	Logger logger;
	BlockingQueue<Message> clientQueue;
	BlockingQueue<Message> heartbeatQueue;

	public IncomingSock(Socket sock, Logger logger, BlockingQueue<Message> leaderQueue,
			BlockingQueue<Message> replicaQueue, BlockingQueue<Message> acceptorQueue,
			BlockingQueue<Message> commanderQueue,
			BlockingQueue<Message> scoutQueue, BlockingQueue<Message> heartbeatQueue)
					throws IOException {
		this.sock = sock;
		in = new ObjectInputStream(sock.getInputStream());
		sock.shutdownOutput();
		this.leaderQueue = leaderQueue;
		this.replicaQueue = replicaQueue;
		this.acceptorQueue = acceptorQueue;
		this.commanderQueue = commanderQueue;
		this.scoutQueue = scoutQueue;
		this.logger = logger;
		this.heartbeatQueue = heartbeatQueue;
	}

	public IncomingSock(Socket sock, Logger logger, 
			BlockingQueue<Message> clientQueue) throws IOException {
		this.sock = sock;
		in = new ObjectInputStream(sock.getInputStream());
		sock.shutdownOutput();
		this.logger = logger;
		this.clientQueue = clientQueue;
	}

	public void run() {
		while (!shutdownSet) {
			try {
				Message msg = (Message) in.readObject();
				addMessageToDestinationQueue(msg);
				logger.info("\n\nReceived : "+msg.toString());
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		shutdown();
	}

	private void addMessageToDestinationQueue(Message msg) {
		switch(msg.getDestType()){
			case ACCEPTOR:
				acceptorQueue.add(msg);
				logger.info("Added to acceptor queue Message :"+ msg.toString());
				break;
			case CLIENT:
				clientQueue.add(msg);
				logger.info("Added to client queue Message :"+ msg.toString());
				break;
			case COMMANDER:
				commanderQueue.add(msg);
				logger.info("Added to commander queue Message :"+ msg.toString());
				break;
			case LEADER:
				leaderQueue.add(msg);
				logger.info("Added to leader queue Message :"+ msg.toString());
				break;
			case REPLICA:
				replicaQueue.add(msg);
				logger.info("Added to replica queue Message :"+ msg.toString());
				break;
			case SCOUT:
				scoutQueue.add(msg);
				logger.info("Added to scout queue Message :"+ msg.toString());
				break;
			case SERVER:
				heartbeatQueue.add(msg);
				logger.info("Added to heartbeat queue Message :"+ msg.toString());
				break;
		}
		
	}

	public void cleanShutdown() {
		shutdownSet = true;
	}

	protected void shutdown() {
		try {
			in.close();
		} catch (IOException e) {
		}

		try {
			sock.shutdownInput();
			sock.close();
		} catch (IOException e) {
		}
	}
}
