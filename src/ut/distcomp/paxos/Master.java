package ut.distcomp.paxos;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

import ut.distcomp.framework.Config;

public class Master {

	static Server[] servers = null;
	static Client[] clients = null;

	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		int numNodes, numClients;

		while (scan.hasNextLine()) {
			String[] inputLine = scan.nextLine().split(" ");
			int clientIndex, nodeIndex;
			System.out.println(inputLine[0]);
			switch (inputLine[0]) {
			case "start":
				numNodes = Integer.parseInt(inputLine[1]);
				numClients = Integer.parseInt(inputLine[2]);
				initializeClients(numNodes, numClients);
				initializeServers(numNodes, numClients);
				startServers();
				break;
			case "sendMessage":
				clientIndex = Integer.parseInt(inputLine[1]);
				String message = "";
				for (int i = 2; i < inputLine.length; i++) {
					message += inputLine[i];
					if (i != inputLine.length - 1) {
						message += " ";
					}
				}
				/*
				 * Instruct the client specified by clientIndex to send the
				 * message to the proper paxos node
				 */
				clients[clientIndex].sendMessageToChatroom(message);
				break;
			case "printChatLog":
				clientIndex = Integer.parseInt(inputLine[1]);
				/*
				 * Print out the client specified by clientIndex's chat history
				 * in the format described on the handout.
				 */
				break;
			case "allClear":
				/*
				 * Ensure that this blocks until all messages that are going to
				 * come to consensus in PAXOS do, and that all clients have
				 * heard of them
				 */
				break;
			case "crashServer":
				nodeIndex = Integer.parseInt(inputLine[1]);
				/*
				 * Immediately crash the server specified by nodeIndex
				 */
				break;
			case "restartServer":
				nodeIndex = Integer.parseInt(inputLine[1]);
				/*
				 * Restart the server specified by nodeIndex
				 */
				break;
			case "timeBombLeader":
				int numMessages = Integer.parseInt(inputLine[1]);
				/*
				 * Instruct the leader to crash after sending the number of
				 * paxos related messages specified by numMessages
				 */
				break;
			}
		}
	}

	private static void startServers() {
		for (Server serv : servers) {
			serv.StartServer();
		}
	}

	private static void initializeClients(int numNodes, int numClients) {
		clients = new Client[numClients];
		for (int i = 0; i < numClients; i++) {
			try {
				clients[i] = new Client(i,new Config(i + numNodes, 
						numNodes, numClients, "LogClient" + i + ".txt"));
			} catch (IOException e) {

			}
		}
	}

	private static void initializeServers(int numServers, int numClients) {
		servers = new Server[numServers];
		for (int i = 0; i < numServers; i++) {
			try {
				servers[i] = new Server(i, new Config(i, numServers, numClients,
						"LogServer"+i+".txt"));
			} catch (IOException e) {
				//e.printStackTrace();
			}
		}
	}
}
