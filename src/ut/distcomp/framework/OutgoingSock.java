/**
 * This code may be modified and used for non-commercial 
 * purposes as long as attribution is maintained.
 * 
 * @author: Isaac Levy
 */

package ut.distcomp.framework;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;

import ut.distcomp.paxos.Message;

public class OutgoingSock {
	final static byte[] MSG_SEP = "&".getBytes();
	Socket sock;
	ObjectOutputStream out;
	
	protected OutgoingSock(Socket sock, ObjectOutputStream outputStream) throws IOException {
		this.sock = sock;
		out = outputStream;
		sock.shutdownInput();
	}
	
	/** 
	 * Do not use '&' character.  This is a hardcoded separator
	 * @param msg
	 * @throws IOException 
	 */
	protected synchronized void sendMsg(Message msg) throws IOException {
		out.writeObject(msg);
		out.flush();
	}
	
	public synchronized void cleanShutdown() {
		try { out.close(); } 
		catch (IOException e) {}

		try { 
			sock.shutdownOutput();
			sock.close(); 
		} catch (IOException e) {}
	}
}