package ut.distcomp.paxos;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import ut.distcomp.framework.Config;

public class Master {

	static Server[] servers = null;
	static Client[] clients = null;
	// TODO: Remove sysouts
	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		int numNodes = 0, numClients = 0;

		while (scan.hasNextLine()) {
			String line = scan.nextLine();
			if (line.compareTo("exit") == 0) {
				break;
			}
			String[] inputLine = line.split(" ");
			int clientIndex, nodeIndex;
			System.out.println(inputLine[0]);
			switch (inputLine[0]) {
			case "start":
				numNodes = Integer.parseInt(inputLine[1]);
				numClients = Integer.parseInt(inputLine[2]);
				start(numNodes, numClients);
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
				printChatlog(clientIndex);
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
				allClear(numNodes, numClients);
				break;
			case "crashServer":
				nodeIndex = Integer.parseInt(inputLine[1]);
				servers[nodeIndex].CrashServer();
				/*
				 * Immediately crash the server specified by nodeIndex
				 */
				break;
			case "restartServer":
				nodeIndex = Integer.parseInt(inputLine[1]);
				servers[nodeIndex] = null;
				servers[nodeIndex] = initializeSingleServer(nodeIndex, numNodes,
						numClients);
				servers[nodeIndex].RestartServer();
				/*
				 * Restart the server specified by nodeIndex
				 */
				break;
			case "timeBombLeader":
				int numMessages = Integer.parseInt(inputLine[1]);
				for (int i = 0; i < numNodes; i++) {
					if (servers[i].IsServerAlive()) {
						servers[i].timeBombLeader(numMessages);
					}
				}
				break;
			}
		}
		for (int i = 0; i < numClients; i++) {
			if (clients[i] != null) {
				clients[i].CrashClient();
			}
		}
		for (int i = 0; i < numNodes; i++) {
			if (servers[i] != null) {
				servers[i].CrashServer();
			}
		}
		System.exit(0);
		return;
	}
	
	private static void start(int numNodes, int numClients){
		initializeClients(numNodes, numClients);
		initializeServers(numNodes, numClients);
		startServers();
	}

	private static void printChatlog(int clientIndex) {
		List<String> chatLog = clients[clientIndex].getChatLog();
		for (String message : chatLog) {
			System.out.println(message);
		}
	}

	private static void allClear(int numServers, int numClients) {
		List<Integer> aliveServers = getAliveServers(numServers);
		System.out.println("\n\nAlive Servers");
		for (Integer integer : aliveServers) {
			System.out.println(integer);
		}
		if (aliveServers.size() > (numServers / 2)) {
			waitForServersToFinishProtocol(aliveServers, numServers);
			waitForAllClientsToBeServiced(numClients);
		}
		// TODO(klad): Check this ?
//		try {
//			Thread.sleep(Config.RevivalDelay + 2 * Config.HeartbeatFrequency);
//		} catch (InterruptedException e) {
//		}
	}

	private static void waitForAllClientsToBeServiced(int numClients) {
		// Check if all clients are serviced
		for (int i = 0; i < numClients; i++) {
			while (!clients[i].allCommandsExecuted()) {
				// Do nothing while all clients have got decisions
				// for all their commands.
			}
			 System.out.println("All commands for client " + i + " aredone.");
		}
	}

	private static List<Integer> getAliveServers(int numServers) {
		List<Integer> aliveServers = new ArrayList<>();
		for (int i = 0; i < numServers; i++) {
			if (servers[i].IsServerAlive()) {
				aliveServers.add(i);
			}
		}
		return aliveServers;
	}

	private static void waitForServersToFinishProtocol(
			List<Integer> aliveServers, int numServers) {
		if (aliveServers.size() > (numServers / 2)) {
			// There is a majority alive.
			// Check whether all have completed protocol related messages.
			for (Integer index : aliveServers) {
				while (servers[index].IsServerAlive()
						&& servers[index].IsServerExecutingProtocol()) {
					// Do nothing till the server is executing protocol.
				}
				 System.out.println("Alive server " + index + " has finished.");
			}
		} else {
			// There is a minority. Cannot continue with protocol.
			// System.out.println("There is no majority. All clear");
			return;
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
				clients[i] = new Client(i, new Config(i + numNodes, numNodes,
						numClients, "LogClient" + i + ".txt"));
			} catch (IOException e) {

			}
		}
	}

	private static void initializeServers(int numServers, int numClients) {
		servers = new Server[numServers];
		for (int i = 0; i < numServers; i++) {
			servers[i] = initializeSingleServer(i, numServers, numClients);
		}
	}

	private static Server initializeSingleServer(int serverId, int numServers,
			int numClients) {
		try {
			return new Server(serverId, new Config(serverId, numServers,
					numClients, "LogServer" + serverId + ".txt"));
		} catch (IOException e) {

		}
		return null;
	}
}
