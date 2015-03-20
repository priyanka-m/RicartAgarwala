import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class RicartAgarwalaServer extends Thread
{
  private ServerSocket serverSocket;
  private DataInputStream in;
  private DataOutputStream out;

  public RicartAgarwalaServer(int port) throws IOException
  {
    serverSocket = new ServerSocket(port);
  }

  public void closeServer() {
   try {
     System.out.println("~~~~~~~~~~~~~~~~MESSAGES EXCHANGED~~~~~~~~~~~~~~~~");
     System.out.println("Messages sent by this node: " + RicartAgarwalaDriver.messagesSent);
     System.out.println("Messages received by this node: " + RicartAgarwalaDriver.messagesRecvd);
     System.out.println("~~~~~~~~~~~~~~~~SHUTTING MYSELF DOWN~~~~~~~~~~~~~~~~");
      serverSocket.close();
    } catch (IOException e) {
     System.out.println("Socket Closed");
    }
  }
  public void run()
  {
    while(true)
    {
      try
      {
        // Accept client connections.
        Socket server = serverSocket.accept();
        in = new DataInputStream(server.getInputStream());
        // Read the requests by the clients
        String s = in.readUTF();

        if (s.length() > 0) {
          RicartAgarwalaDriver.messagesRecvd += 1;
          String[] decodeRequest = s.split(",");

          // If received request for a CS.
          if (decodeRequest[0].equals("REQUEST")) {
            RicartAgarwalaDriver.highest_sequence_number =
                Math.max(RicartAgarwalaDriver.highest_sequence_number, Integer.parseInt(decodeRequest[1]));

            /* If the receiving node needs the CS too and its sequence number is lower or its node id is lower,
              it will defer the incoming request.
            */
            if (RicartAgarwalaDriver.requesting_critical_section
                && (Integer.parseInt(decodeRequest[1]) > RicartAgarwalaDriver.our_sequence_number
                || (Integer.parseInt(decodeRequest[1]) == RicartAgarwalaDriver.our_sequence_number
                && Integer.parseInt(decodeRequest[2]) > RicartAgarwalaDriver.nodeId))) {

              RicartAgarwalaDriver.reply_deferred[Integer.parseInt(decodeRequest[2])] = true;
              out = new DataOutputStream(server.getOutputStream());
              out.writeUTF("DEFERRED");
              //RicartAgarwalaDriver.messagesSent += 1;

            } else {
              RicartAgarwalaDriver.token[Integer.parseInt(decodeRequest[2])] = false;
              out = new DataOutputStream(server.getOutputStream());
              out.writeUTF("GRANTED");
              RicartAgarwalaDriver.messagesSent += 1;
            }

            // If receiving replies for requests deferred in the past.
          } else if (decodeRequest[0].equals("GRANTED")) {
            RicartAgarwalaDriver.msgsCurrCS += 1;
            RicartAgarwalaDriver.token[Integer.parseInt(decodeRequest[1])] = true;
            RicartAgarwalaDriver.outstanding_reply_count -= 1;

            // If receiving execution completion.
          } else if (decodeRequest[0].equals("COMPLETED")) {
            RicartAgarwalaDriver.completionReports += 1;
          }
        }
        server.close();
      }catch(SocketTimeoutException s)
      {
        System.out.println("Socket timed out!");
        break;

      } catch(IOException e) {
        System.out.println("~~~~~~~Algorithm Ends~~~~~~");
        break;
      }
    }
  }
}

