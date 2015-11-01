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
	HashMap<Integer, BlockingQueue<Message>> commanderQueue;
	HashMap<Integer, BlockingQueue<Message>> scoutQueue;
	Logger logger;
	BlockingQueue<Message> clientQueue;
	BlockingQueue<Message> heartbeatQueue;
	
	public IncomingSock(Socket sock, Logger logger, BlockingQueue<Message> leaderQueue,
			BlockingQueue<Message> replicaQueue, BlockingQueue<Message> acceptorQueue,
			HashMap<Integer, BlockingQueue<Message>> commanderQueue,
			HashMap<Integer, BlockingQueue<Message>> scoutQueue,
			BlockingQueue<Message> heartbeatQueue) throws IOException {
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

	public IncomingSock(Socket sock, Logger logger, BlockingQueue<Message> clientQueue) throws IOException {
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
				logger.info("Received : "+msg.sampleString);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		shutdown();
	}
	
	public void cleanShutdown() {
		shutdownSet = true;
	}
	
	protected void shutdown() {
		try { in.close(); } catch (IOException e) {}
		
		try { 
			sock.shutdownInput();
			sock.close(); }			
		catch (IOException e) {}
	}
}
