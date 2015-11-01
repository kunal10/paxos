/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;

import ut.distcomp.paxos.Message;

public class ListenServer extends Thread {

	public volatile boolean killSig = false;
	int port;
	int procNum;
	List<IncomingSock> socketList;
	Config conf;
	ServerSocket serverSock;
	private BlockingQueue<Message> leaderQueue;
	private BlockingQueue<Message> replicaQueue;
	private BlockingQueue<Message> acceptorQueue;
	private HashMap<Integer, BlockingQueue<Message>> commanderQueue;
	private HashMap<Integer, BlockingQueue<Message>> scoutQueue;
	private BlockingQueue<Message> clientQueue;
	private BlockingQueue<Message> heartbeatQueue;
	
	private void startServerSock(){
		procNum = conf.procNum;
		port = conf.ports[procNum];
		try {
			serverSock = new ServerSocket(port);
			conf.logger.info(String.format(
					"Server %d: Server connection established", procNum));
		} catch (IOException e) {
			String errStr = String.format(
					"Server %d: [FATAL] Can't open server port %d", procNum,
					port);
			conf.logger.log(Level.SEVERE, errStr);
			throw new Error(errStr);
		}
	}

	protected ListenServer(Config conf, List<IncomingSock> sockets) {
		this.conf = conf;
		this.socketList = sockets;
		startServerSock();
	}

	public ListenServer(Config config, List<IncomingSock> inSockets, BlockingQueue<Message> leaderQueue,
			BlockingQueue<Message> replicaQueue, BlockingQueue<Message> acceptorQueue,
			HashMap<Integer, BlockingQueue<Message>> commanderQueue,
			HashMap<Integer, BlockingQueue<Message>> scoutQueue,
			BlockingQueue<Message> heartbeatQueue) {
		this.conf = config;
		this.socketList = inSockets;
		this.leaderQueue = leaderQueue;
		this.replicaQueue = replicaQueue;
		this.acceptorQueue = acceptorQueue;
		this.commanderQueue = commanderQueue;
		this.scoutQueue = scoutQueue;
		this.heartbeatQueue = heartbeatQueue;
		startServerSock();
	}

	public ListenServer(Config config, List<IncomingSock> inSockets, BlockingQueue<Message> clientQueue) {
		this.conf = config;
		this.socketList = inSockets;
		this.clientQueue = clientQueue;
		startServerSock();
	}

	public void run() {
		while (!killSig) {
			try {
				IncomingSock incomingSock = null;
				// Assuming servers are processes with id's less than 1000. All others are clients
				if(procNum < 1000){
					incomingSock = new IncomingSock(serverSock.accept(),
							conf.logger,
							leaderQueue,
							replicaQueue,
							acceptorQueue,
							commanderQueue,
							scoutQueue,
							heartbeatQueue);
				}else {
					incomingSock = new IncomingSock(serverSock.accept(),
							conf.logger,
							clientQueue);
				}
				 
				socketList.add(incomingSock);
				incomingSock.start();
				conf.logger.fine(String.format(
						"Server %d: New incoming connection accepted from %s",
						procNum, incomingSock.sock.getInetAddress()
								.getHostName()));
			} catch (IOException e) {
				if (!killSig) {
					conf.logger.log(Level.INFO, String.format(
							"Server %d: Incoming socket failed", procNum), e);
				}
			}
		}
	}

	protected void cleanShutdown() {
		killSig = true;
		try {
			serverSock.close();
		} catch (IOException e) {
			conf.logger.log(Level.INFO,String.format(
					"Server %d: Error closing server socket", procNum), e);
		}
	}
}
