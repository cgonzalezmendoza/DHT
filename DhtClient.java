/**
 * Authors: Carlos Gonzalez, Nicola Pedretti
 * Washington University in St. Louis
 * CSE 473: Introduction to Computer Networks
 * Lab 3
 *
 * The Dht Client class initializes a Dht client. This program will
 * take from 3 to 5 command line arguments. The first is the IP address
 * of theinterface that the client should bind to its own datagram socket.
 * The second is the name of a configuration file containing the IP address
 * and port number used by a DhtServer (each server writes such a file
 * when it starts up). The third is an operation (get or put) and
 * the remaining arguments specify the key and/or value for the operation.
 * These may be omitted. DhtClient does not do any error checking.
 **/
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;

public class DhtClient {
    
    //Initializes all the parameters for the DhtClient.
    private String filename;
    private String operation;
    private String key;
    private String value;
    private InetAddress myIp;
    private int tag = 12345;
    private int ttl = 100;
    private boolean debug = true;
    private DatagramSocket socket;
    
    //Creating the Generics object for future use.
    private static Generics gen = new Generics();
    
    /**
     * Main function initializing the client.
     * @param args
     *        the command line arguments that run the client.
     */
    public static void main(String[] args){
        
        //Create a new client based on the command line inputs.
        DhtClient client = new DhtClient(args);
        
        //Create and send the packet with the paremeters.
        Packet out = new Packet();
        client.sendToServer(out);
        
        //Receive the response from the DHT
        Packet in = new Packet();
        client.receiveFromServer(in);
        
        //Close the TCP connection.
        client.closeConnection();
        
    }
    /**
     * Initializes all the paremeters of a DHT client object.
     * @param args
     *        commands to input into the client.
     */
    public DhtClient(String[] args) {
        try {
            //Set the client packet's parameters based on input.
            myIp = InetAddress.getByName(args[0]);
            filename = args[1];
            operation = args[2];
            key = args.length < 4 ? null : args[3];
            value = args.length < 5 ? null : args[4];
            
            //Initialize the output socket.
            socket = new DatagramSocket(0, myIp);
        } catch (Exception e) {
            gen.usage(new String[] { "myIp", "serverfile", "operation" },
                    new String[] { "key", "value" }, true);
        }
    }
    /**
     * Sends the packet to the corresponding server.
     * @param out
     *       packet to be send to the client.
     */
    private void sendToServer(Packet out) {
        //Initializing the outgoing packet's values.
        out.type = operation;
        out.key = key;
        out.val = value;
        out.tag = tag;
        out.ttl = ttl;
        out.senderInfo = null;
        
        //Reads the servers information from its configuration file.
        ArrayList<String> info = gen.readLinesFromFile(filename);
        InetSocketAddress server;
        try {
            server = new InetSocketAddress
                        (InetAddress.getByName(info.get(0)),
                                Integer.parseInt(info.get(1)));
            out.send(socket, server, debug);
        } catch (Exception e) {
            System.out.println("Incorrect server IP or port.");
        }
    }
    /**
     * Receives a packet and outputs an error if it fails.
     * @param in
     *       packet to be received
     */
    private void receiveFromServer(Packet in) {
        
        InetSocketAddress sender = null;
        
        //Receive a packet and store its address.
        sender = in.receive(socket, debug);
        
        //Detect any errors.
        if (sender == null || in.tag != tag) {
            System.out.println("Received packet failure");
            System.exit(gen.PACKET_FAILURE);
        }
    }
    /**
     *Closes the socket connection.
     */
    private void closeConnection(){
        socket.close();
    }
    
}
