package ut.distcomp.paxos;
/**
 * Runs the functionality of a replica.  
 * Receives requests from a client.
 * Maintains decision, proposals and state.
 * Sends the proposal to a leader.
 * Broadcasts back to all the clients when it executes a decision from some Commander. 
 * Should also monitor for messages requesting for the current state 
 * which is sent by a process which is revives.
 * On revival, its set of decisions is obtained from other alive servers.
 */
public class Replica {

}
