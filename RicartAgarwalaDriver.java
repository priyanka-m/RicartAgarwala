/**
 * Created by priyanka on 2/20/15.
 */

import java.io.*;
import java.net.ConnectException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

/*  The RicartAgarwalaDriver, is the main class, that runs on top of each nodes,
    and does all the computation, including spawning the server. It also has all
    the information that is referred to as the SHARED DATABASE in the research paper.
* */
public class RicartAgarwalaDriver {
  static int nodeId;
  static int port;
  static int msgsCurrCS = 0;
  static int messagesSent = 0;
  static int messagesRecvd = 0;
  static int oneTimeUnit = 100;
  static int our_sequence_number;
  static final int numNodes = 10;
  static int critical_section_limit = 20;
  static int highest_sequence_number = 0;
  static volatile int completionReports = 0;
  static volatile int outstanding_reply_count;
  static boolean[] token = new boolean[numNodes];
  static boolean requesting_critical_section = false;
  static boolean[] reply_deferred = new boolean[numNodes];
  static ArrayList<String> nodeAddresses = new ArrayList<String>(numNodes);


  /*  Each node has a list of ip addresses of nodes in the system. Hardcoded! Really bad design! Ideally a config file
      should have been good. lack of time.
  * */
  public static void populateNodeAddresses() {
    nodeAddresses.add(0, "dc21.utdallas.edu");
    nodeAddresses.add(1, "dc22.utdallas.edu");
    nodeAddresses.add(2, "dc23.utdallas.edu");
    nodeAddresses.add(3, "dc24.utdallas.edu");
    nodeAddresses.add(4, "dc25.utdallas.edu");
    nodeAddresses.add(5, "dc26.utdallas.edu");
    nodeAddresses.add(6, "dc27.utdallas.edu");
    nodeAddresses.add(7, "dc28.utdallas.edu");
    nodeAddresses.add(8, "dc29.utdallas.edu");
    nodeAddresses.add(9, "dc30.utdallas.edu");
  }

  /*  All nodes except the first node call this function when they
      finish their execution of 40 critical sections. It's purpose is
      to inform the first node of the completion of the calling node.
  * */
  public static void sendCompletionReport() {
    try {
      Socket client = new Socket(nodeAddresses.get(0), RicartAgarwalaDriver.port);
      OutputStream outToServer = client.getOutputStream();
      DataOutputStream out = new DataOutputStream(outToServer);

      System.out.println("Node " + nodeId + ": I'm done");
      out.writeUTF("COMPLETED," + RicartAgarwalaDriver.nodeId);
      client.close();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*  This is called when the critical section is activated.
  * */
  public static void criticalSectionActivated() {
    Date now = new Date();
    System.out.println("Entering CS.....");
    System.out.println(nodeId + " " + System.currentTimeMillis());
    try {
      Thread.sleep(3 * oneTimeUnit);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*  This function is used to reply to nodes, whose requests for critical section
      were previous deferred.
      @param ipAddress: Ip address of the node whose request was deferred
      @param nodeServerID: Node ID of the node whose request was deffered
  * */
  public static void sendDeferredReply(String ipAddress, int nodeServerID) {
    try {
      Socket client = new Socket(ipAddress, port);
      OutputStream outToServer = client.getOutputStream();
      DataOutputStream out = new DataOutputStream(outToServer);
      out.writeUTF("GRANTED," + RicartAgarwalaDriver.nodeId);
      messagesSent += 1;
      // Set your token to false, letting the other node have it, when allowing other node to execute its CS
      token[nodeServerID] = false;
      client.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*  This function lets the client make a request to the server at 'ipAddress'
      @param ipAddress: Ip address of the node to whom we are making a request
      @param nodeServerID: Node ID of the node whom we are making a request
  * */
  public static void clientCall(String ipAddress, int nodeIDServer) {
    try
    {
      Socket client = new Socket(ipAddress, RicartAgarwalaDriver.port);
      OutputStream outToServer = client.getOutputStream();
      DataOutputStream out = new DataOutputStream(outToServer);
      out.writeUTF("REQUEST,"+ RicartAgarwalaDriver.our_sequence_number + "," + RicartAgarwalaDriver.nodeId);

      messagesSent += 1;
      msgsCurrCS += 1;

      InputStream inFromServer = client.getInputStream();
      DataInputStream in = new DataInputStream(inFromServer);

      while (!(in.available() > 0)) {}

      if (in.readUTF().equals("GRANTED")) {
        messagesRecvd += 1;
        msgsCurrCS += 1;
        RicartAgarwalaDriver.token[nodeIDServer] = true;
        RicartAgarwalaDriver.outstanding_reply_count -= 1;
      }
      client.close();

    } catch (ConnectException c) {
      // Take the token when receiving a reply to your request
      RicartAgarwalaDriver.token[nodeIDServer] = true;
      RicartAgarwalaDriver.outstanding_reply_count -= 1;

    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  /*  This function requests all the nodes in the system, for entry into its critical section.
      Only those nodes are sent a request who currently have the token with them
  * */
  public static void requestCS() {
    try {
      for (int i = 0; i < nodeAddresses.size(); i++) {
        if (i != nodeId) {
          if (!token[i]) {
            clientCall(nodeAddresses.get(i), i);
          } else {
            outstanding_reply_count -= 1;
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  /*  This function takes all the steps required to request, execute, and reply to deffered messages
      associated with a critical section.
  * */
  public static void stepsForCS() {
    // wait for 5-10 units of time before requesting a CS
    int START = 5, END = 10;
    Random random = new Random();
    try {
      Thread.sleep(generateRandomNumber(START, END, random) * oneTimeUnit);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Begin the actual computation
    requesting_critical_section = true;
    our_sequence_number = highest_sequence_number + 1;
    outstanding_reply_count = numNodes - 1;

    // We need to keep a record of the elapsed time.
    long currTime = System.nanoTime();

    // Call this function to send request to all (numNodes - 1) nodes in the system.
    requestCS();

    // Wait until you have received a reply from everyone.
    while (outstanding_reply_count > 0) {}

    long currTime2 = System.nanoTime();

    System.out.println("Time elapsed while making requests for CS: " + ((currTime2 - currTime)/1000000) + " milisecond(s)");

    // When in CS, execute these steps.
    criticalSectionActivated();

    // You no longer need this privilege
    requesting_critical_section = false;

    // Reply to all the nodes whose requests were deferred.
    for (int i = 0; i < numNodes; i++) {
      if (reply_deferred[i] && i != nodeId) {
        sendDeferredReply(nodeAddresses.get(i), i);
        reply_deferred[i] = false;
      }
    }
  }

  // Generates a random number between the range [aStart, aEnd]
  private static int generateRandomNumber(int aStart, int aEnd, Random aRandom){
    if (aStart > aEnd) {
      throw new IllegalArgumentException("Start cannot exceed End.");
    }
    //get the range, casting to long to avoid overflow problems
    long range = (long)aEnd - (long)aStart + 1;
    // compute a fraction of the range, 0 <= frac < range
    long fraction = (long)(range * aRandom.nextDouble());
    return  (int)(fraction + aStart);
  }

  /*  The main function that is the crux of the whole algorithm.
  * */
  public static void main(String[] args) {

    try {
      nodeId = Integer.parseInt(args[0]);
      port = Integer.parseInt(args[1]);

      // Fill in the hardcoded IP addresses into an arraylist.
      populateNodeAddresses();

      // Start the server
      RicartAgarwalaServer server = new RicartAgarwalaServer(port);
      server.start();

      /* Wait until all nodes have started. This is bad design! Could have done something neat.
        Might come with a shell script when giving the demo, which starts all the nodes together.
        Hence will comment this out in the demo!
       */
      Thread.sleep(10000);

      // Phase: 1
      int csCount = 1;
      while (csCount <= critical_section_limit) {
        System.out.println("~~~~~~~~~~~~~~~~NODE: " + nodeId  + " CRITICAL SECTION NUMBER: " + csCount + "~~~~~~~~~~~~~~~~");
        // Keep count of messages exchanged for executing the CS.
        msgsCurrCS = 0;
        stepsForCS();
        System.out.println("Messages Exchanged: " + msgsCurrCS);
        csCount++;
      }

      // Phase: 2
      csCount = 21;
      while (csCount - 20 <= critical_section_limit) {
        System.out.println("~~~~~~~~~~~~~~~~NODE: " + nodeId  + " CRITICAL SECTION NUMBER: " + csCount + "~~~~~~~~~~~~~~~~");
        // Keep count of messages exchanged for executing the CS.
        msgsCurrCS = 0;
        stepsForCS();
        System.out.println("Messages Exchanged: " + msgsCurrCS);
        csCount++;
        if (nodeId % 2 == 0) {
          // wait for a random unit of time between the range [45-50]
          Random random = new Random();
          Thread.sleep(generateRandomNumber(45, 50, random));
        }
      }

      // All nodes except the 0th node send completion reports to the 0th node and shut themselves down.
      if (nodeId != 0) {
        sendCompletionReport();
        server.closeServer();
      } else {
        completionReports += 1;
        while (completionReports != numNodes) {}
        System.out.println("~~~~~~~~~~SHUTTING DOWN~~~~~~~~~~~");
        server.closeServer();
      }

    } catch (Exception e) {
      System.out.println("~~~~~~~~~~Algorithm ends~~~~~~~~~~~~");
    }
  }
}
