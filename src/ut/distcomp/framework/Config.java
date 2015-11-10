/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class Config {

	/**
	 * Loads config from a file. Optionally puts in 'procNum' if in file. See
	 * sample file for syntax
	 * 
	 * @param filename
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Config(int procNum, int numServers, int numClients, String logfile)
			throws IOException {
		this.procNum = procNum;
		this.numClients = numClients;
		this.numServers = numServers;
		this.numProcesses = numServers + numClients;
		logger = Logger.getLogger("NetFramework" + procNum);
		FileHandler fileHandler = new FileHandler(logfile);
		logger.addHandler(fileHandler);
		logger.setUseParentHandlers(false);
		fileHandler.setFormatter(new MyFormatter());
		addresses = new InetAddress[numProcesses];
		ports = new int[numProcesses];
		int basePort = 5000;
		for (int i = 0; i < numProcesses; i++) {
			ports[i] = basePort + i;
			addresses[i] = InetAddress.getByName("localhost");
		}
	}

	/**
	 * Default constructor for those who want to populate config file manually
	 */
	public Config() {
	}

	/**
	 * Array of addresses of other hosts. All hosts should have identical info
	 * here.
	 */
	public InetAddress[] addresses;

	/**
	 * Array of listening port of other hosts. All hosts should have identical
	 * info here.
	 */
	public int[] ports;

	/**
	 * Total number of hosts
	 */
	public int numProcesses;

	/**
	 * This hosts number (should correspond to array above). Each host should
	 * have a different number.
	 */
	public int procNum;

	/**
	 * Logger. Mainly used for console printing, though be diverted to a file.
	 * Verbosity can be restricted by raising level to WARN
	 */
	public Logger logger;

	/**
	 * No of clients
	 */
	public int numClients;

	/**
	 * Num of servers.
	 */
	public int numServers;

	public static final int HeartbeatTimeout = 1200;
	
	public static final int HeartbeatFrequency = 1000;
	
	public static final int RevivalDelay = HeartbeatTimeout * 2;
}

class MyFormatter extends Formatter {

	@Override
	public String format(LogRecord record) {
		return record.getLevel() + ":" + record.getMessage() + "\n";
	}
}
