                     DISTRIBUTED COMPUTING (CS 380D)
                           PROJECT 2 : PAXOS

SLIP DAYS:
Used for this project 2
Total used for all projects 4

TEAM INFO:
  
Name                    UT Eid    CS Id    
1. KUNAL LAD            KL28697   klad@cs.utexas.edu
2. ASHWINI VENKATESH    AV28895   ashuven6@cs.utexas.edu

A) Commands to run:

# We have changed the timeout for a test case in tester.py to 200s. If you are 
# not using the modified tester.py in our source code, please change the timeout 
# from 10s to 200s.
 
# Assuming that you are in paxos directory obtained by unzipping the zip file.
cd src

# This script will build paxos project and run tester.py.
# tester.py expects tests and solution files in src/tests and src/solutions.
./executePaxos.sh 

B) MASTER API:

1. start [numberOfServers] [numberOfClients] :
   This command starts a chat room with a specified number of servers and 
   clients. The servers and clients will be referred to by their index number 
   (zero indexed).

2. sendMessage [index] message here :
   This command tells a client (specified by the index) to send a message to the
   chat room. The message will be every argument after the specified index. The
   call is asynchronous.


3. crashServer [index] : 
   The will shutdown a server immediately.

4. restartServer [index] : 
   This will restart a server that we have previously crashed.

5. allClear :
   This command blocks until the chat room system is idle. This combined with
   the timeBombLeader command allows us to test failures in the middle of the
   consensus protocol.

6. timeBombLeader [numberOfMsgs] :
   This command tells the current leader to crash itself after sending the
   specified number of server side Paxos related messages. This excludes only
   P1A and P2B messages.

7. printChatLog [index]
   This command (blocks until completion) will print a client’s view of the chat
   record in the format specified below
   
   [sequenceNumber] [senderIndex]: message

C) PAXOS DESIGN:

HEIRARCHY OF CLASSES : Indentation indicates the hierarchy.

Master
Client
Server
    -Replica
    -Acceptor
    -Leader
        -Commander
        -Scout

D) MAJOR DESIGN DECISIONS:

1. HeartBeat:
   
   Each server sends a heart beat to all other server every 1s. If a server does
   not receive heart beat from some other server then it concludes that it is 
   dead. Since our system is an Asynchronous system, it is POSSIBLE that:
   
   - A server detects false deaths.
     This can lead to multiple primary leaders being elected.
     
   - A server never detects death of other processes (If it comes back up before 
     death could be detected).
     This does not cause any problems.

2. Leader Election:

   We are using a round robin strategy for leader election. It is possible that
   in an asynchronous system this can lead to multiple primary leaders being
   present. This can lead to RACING between multiple primary leaders.

3. Racing between leaders:

   We are using the strategy of randomized back off to ensure that the protocol
   can make progress even if there are multiple primary leaders.
   
4. Recovery:

   - Acceptor
     * On restart acceptor sends a STATE_REQ message to all the replicas.
     * Waits for some stable acceptor to respond back with its accepted set. 
     * Sets its accepted set to the accepted set in STATE_RES message.
    
     NOTE : Paxos ensures that if a replica proposed something for a slot, then
     it does not propose something different in same slot with higher ballot.

   - Replica
     * On restart replica sends a STATE_REQ message to all the replicas.
     * Waits for some stable replica to respond back. 
     * Sets its state to the received state in STATE_RES message. 
     
     NOTE: In asynchronous system it is possible that the replica sending 
     STATE_RES received a proposal or decision (which was sent when recovering 
     replica was dead) after sending the STATE_RES. Recovering replica will miss 
     this, but it will learn about it through Paxos protocol when it tries to
     propose something.
       
5. AllClear  

   - If less than a majority of servers is alive then master waits for 2 secs.
   
   - If majority of servers are running then:
     i) Wait for all commanders & scout threads to finish executing the protocol
        NOTE : Randomized back off ensures that these threads will be able to
               finish executing the protocol.
     ii) Wait for all clients to receive all the decisions.

