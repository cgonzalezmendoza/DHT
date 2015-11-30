 /**
  * Authors: Carlos Gonzalez, Nicola Pedretti
  * Washington University in St. Louis
  * CSE 473: Introduction to Computer Networks
  * Lab 3
  *
  *
  * Server for simple distributed hash table that stores (key,value) strings.
  *
  *  Inputs: DhtServer myIp numRoutes cfgFile [ cache ] [ debug ] [ predFile ]
  *
  *  myIp	is the IP address to use for this server's socket
  *  numRoutes	is the max number of nodes allowed in the DHT's routing table;
  *  		typically lg(numNodes)
  *  cfgFile	is the name of a file in which the server writes the IP
  *		address and port number of its socket
  *  cache	is an optional argument; if present it is the literal string
  *		"cache"; when cache is present, the caching feature of the
  *		server is enabled; otherwise it is not
  *  debug	is an optional argument; if present it is the literal string
  *		"debug"; when debug is present, a copy of every packet received
  *		and sent is printed on stdout
  *  predFile	is an optional argument specifying the configuration file of
  *		this node's predecessor in the DHT; this file is used to obtain
  *		the IP address and port number of the precessor's socket,
  *		allowing this node to join the DHT by contacting predecessor
  *
  *  The DHT uses UDP packets containing ASCII text. Here's an example of the
  *  UDP payload for a get request from a client.
  *
  *  CSE473 DHTPv0.2015
  *  type:get
  *  key:dungeons
  *  tag:12345
  *  ttl:100
  *
  *  The first line is just an identifying string that is required in every
  *  DHT packet. The remaining lines all start with a keyword and :, usually
  *  followed by some additional text. Here, the type field specifies that
  *  this is a get request; the key field specifies the key to be looked up;
  *  the tag is a client-specified tag that is returned in the response; and
  *  can be used by the client to match responses with requests; the ttl is
  *  decremented by every DhtServer and if <0, causes the packet to be
  * discarded.
  *
  *  Possible responses to the above request include:
  *
  *  CSE473 DHTPv0.2015
  *  type:success
  *  key:dungeons
  *  value:dragons
  *  tag:12345
  *  ttl:95
  *
  *  or
  *
  *  CSE473 DHTPv0.2015
  *  type:no match
  *  key:dungeons
  *  tag:12345
  *  ttl:95
  *
  *  Put requests are formatted similarly, but in this case the client
  * typically specifies a value field (omitting the value field causes
  * the pair with the specified key to be removed).
  *
  *  The packet type "failure" is used to indicate an error of some sort;
  *  in this case, the "reason" field provides an explanation of the failure.
  *  Other packet types are listed below. The "join" type is used by a server
  *  to join an existing DHT. The "transfer" is used to transfer (key,value)
  *  pairs to a newly added server.
  *
  *  Other fields and their use are described briefly below
  *
  *  clientAdr 	is used to specify the IP address and port number of the client
  *  	  	that sent a particular request; it is added to a request packet
  *  	  	by the first server to receive the request, before forwarding
  *  	  	the packet to another node in the DHT; an example of the format
  *  	  	is 123.45.67.89:51349
  *  relayAdr  	is used to specify the IP address and port number of the first
  *  	  	server to receive a request packet from the client; it is added
  *  	  	to the packet by the first server before forwarding the packet
  *  hashRange 	is a pair of integers separated by a colon, specifying a range
  *  	  	of hash indices; it is included in the response to a "join"
  *  	  	packet, to inform the new DHT server of the set of nodes
  *  	  	it is responsible for
  *  succInfo  	is the IP address and port number of a server, followed by its
  *  	  	first hash index; this information is included in the response
  *  	  	to a join packet to inform the new DHT server about its
  *		immediate successor; an example of the format is
  *		123.45.6.7:5678:987654321
  *  senderInfo is the IP address and port number of a DHT server,
  *             followed by its first hash index; this information is sent
  *             by a DHT toprovide routing information that can be used by
  *             another
  */

import java.awt.Window.Type;
import java.io.*;
import java.net.*;
import java.util.*;

import javax.swing.text.html.HTMLDocument.HTMLReader.IsindexAction;

public class DhtServer {
    
    //The String containing our predecessor file.
    private String predFile;
    //The String which will store the servers IP and Port number.
    private String cnfgFile;
    
    private int numRoutes; // number of routes in routing table
    private boolean cacheOn; // enables caching when true
    private boolean debug; // enables debug messages when true
    
    private HashMap<String, String> map; // key/value pairs
    private HashMap<String, String> cache; // cached pairs
    private List<Pair<InetSocketAddress, Integer>> rteTbl;//Routing Table
    
    private DatagramSocket sock;
    private InetSocketAddress myAdr;
    private InetSocketAddress predecessor; // DHT predecessor
    private Pair<InetSocketAddress, Integer> succInfo; // successor
    private Pair<Integer, Integer> hashRange; // my DHT hash range
    private int sendTag; // tag for new outgoing packets
    //Custom class in order to use functions across DhtClient & DhtServer.
    private Generics gen = new Generics();
    
    /**
     * Main method for the DHT server. Creates a class of the DhtServer
     * Object, with the given command line arguments. It then initializes
     * the server.
     */
    public static void main(String[] args) {
        DhtServer server = new DhtServer(args);
        server.startServer();
    }
    
    /**
     *The DhtServer constructor, initialized with the command line
     * arguments as described in the class description in the beginning.
     */
    public DhtServer(String[] args) {
        try {
            
            //Initiializing all the parameters of the DhtServer class:
            
            //The server's IP address.
            InetAddress myIp = InetAddress.getByName(args[0]);
            numRoutes = Integer.parseInt(args[1]); //Routes in table.
            cnfgFile = args[2]; //File to store server's IP and port.
            cacheOn = debug = false; //Default false for cache and debug.
            sendTag=6578;//Default sendTag for the server.
            map = new HashMap<String, String>(); //Map of key,value pairs.
            cache = new HashMap<String, String>(); //Map of cache.
            
            //The routing table to store server shortcuts.
            rteTbl = new LinkedList<Pair<InetSocketAddress, Integer>>();
            
            //The socket from which to listen for clients.
            sock = new DatagramSocket(0, myIp);
            
            //The server's unique IP,port pair.
            myAdr = new InetSocketAddress(myIp, sock.getLocalPort());
            
            //Range of values allowed to be mapped in this server.
            hashRange = new Pair<Integer, Integer>(0, Integer.MAX_VALUE);
            
            //Reading the rest of the optional command line arguments.
            for (int i = 3; i < args.length; i++) {
                if (args[i].equals("cache"))
                    cacheOn = true;
                else if (args[i].equals("debug"))
                    debug = true;
                else
                    predFile = args[i];
            }
            //Catching exceptions in case of failure to create server.
        } catch (Exception e) {
            gen.usage(new String[] { "DhtServer", "myIp", "numRoutes",
                "cfgFile" }, new String[] { "debug", "predFile" }, true);
        }
    }
    
    /**
     *This function initiates the server itself, stores its config file,
     * and begins to listen for clients.
     */
    public void startServer() {
        //Writing IP,port to the config file.
        writeMyConfigFile();
        //Configure the server's predecessor.
        configPredecessor();
        //Begin listening for clients.
        listenForClients();
    }
    
    /**
     *Writes IP and port to config file.
     */
    private void writeMyConfigFile() {
        
        //Creating the lines to be written.
        ArrayList<String> lines = new ArrayList<String>();
        lines.add(myAdr.getAddress().getHostAddress());
        lines.add("" + myAdr.getPort());
        
        //Storing the IP,Port in the file.
        gen.writeLinesToFile(lines, cnfgFile);
    }
    
    /**
     * Configures the predecessor if predFile is specified, or sets
     * the successor to null otherwise.
     */
    private void configPredecessor() {
        if (predFile == null){
            //Successor is itself since its the first server to be created.
            succInfo = new Pair<InetSocketAddress, Integer>(myAdr, 0);
        }
        else{
            //Setting the predecessor by reading the file.
            ArrayList<String> predServerInfo
                    = gen.readLinesFromFile(predFile);
            predecessor = new InetSocketAddress(predServerInfo.get(0),
                    Integer.parseInt(predServerInfo.get(1)));
            
            //Joining the predecessor server.
            join(predecessor);
        }
    }
    
    /**
     * Join an existing DHT.
     * @param predAdr
     *            is the socket of the already existing DHT server.
     */
    private void join(InetSocketAddress predAdr) {
        
        //Creating the outgoing packet with type join and tag sendTag.
        Packet out = new Packet();
        out.type = "join";
        out.tag = sendTag;
        
        //Sending the join packet.
        out.send(sock, predAdr, debug);
        
        //Creating the receiving packet.
        Packet in = new Packet();
        InetSocketAddress sender = null;
        
        //Reading the receiving packet.
        sender = in.receive(sock, debug);
        
        //Checking for packet receiving errors.
        if (sender == null ) {
            System.out.println("Received packet failure.");
            System.exit(gen.ERROR_JOINING);
        }
        
        //Upon succesfull joining of another server.
        if (in.type.equals("success")) {
            
            //Setting the server's hashRange to the one specified by the
            //original server's packet.
            hashRange.left = in.hashRange.left.intValue();
            hashRange.right = in.hashRange.right.intValue();
            
            //Setting the succesor of this server to the one specified
            //by the previous server.
            succInfo = new Pair<InetSocketAddress, Integer>(new InetSocketAddress(in.succInfo.left.getAddress(),in.succInfo.left.getPort()),in.succInfo.right.intValue());
			addRoute(succInfo); 
        }
    }
    
    /**
     *Server now begins to listen for client's requests.
     */
    private void listenForClients() {
        
        //Initializing the input packet from a client.
        Packet in = new Packet();
        
        //Initializing the sender's address.
        InetSocketAddress sender = null;
        while (true) {
            //Receive the packet from a client.
            sender = in.receive(sock, debug);
            
            //Checking packet receiving failure.
            if (sender == null) {
                System.out.println("received packet failure");
                continue;
            }
            
            //Checking the packet for any error in its syntax.
            if (!in.check())
                //Syntax error's return a packet with the reason
                //for the error.
                alertFailure(sender, in);
            
            else{
                //Checking to see if the packet has a route to be added.
                if( !(in.senderInfo == null)){
                    addRoute(in.senderInfo);
                }
                
                //Handle the packet according to its type.
                handlePacket(in, sender);
            }
        }
    }
    
    /**
     *Returns a failure packet to the sender.
     * @param dest
     *           The destination of the packet.
     * @param in
     *          The packet to be returned.
     */
    private void alertFailure(InetSocketAddress dest, Packet in) {
        //Creating and setting the reply packet.
        Packet reply = new Packet();
        reply.type = "failure";
        reply.reason = in.reason;
        reply.tag = in.tag;
        reply.ttl = in.ttl;
        
        //sending the failure packet.
        reply.send(sock, dest, debug);
    }
    
    /**
     * Handle packets received from clients or other servers
     *
     * @param in
     *            packet to be analyzed.
     * @param sender
     *            the address of the packet's sender.
     */
    private void handlePacket(Packet in, InetSocketAddress sender) {
        //Checking the type of the packet, and calling the responsible
        //function for it.
        if (in.type.equals("transfer")) {
            handleXfer(in);
            return;
        }
        else if (in.type.equals("join")) {
            handleJoin(in, sender);
            return;
        }
        else if (in.type.equals("success") || in.type.equals("no match")) {
            //Add to cache if successful or no match and cache is set.
            addToCache(in);
            //reply the packet.
            sendBack(in, in.clientAdr);
            return;
        }
        //Checking if the packet is outside the server's assigned range.
        else if (!isRequestInRange(in)) {
            //If the key is in cache, return its value.
            if(in.type.equals("get") && getFromCache(in)){
                sendBack(in,sender);
            }
            //Otherwise forward the packet
            else
                forward(in, sender);
            return;
        }
        else if (in.type.equals("get"))
            handleGet(in);
        else if (in.type.equals("put"))
            handlePut(in);
        
        //After modifying the packet accordingly, return it.
        sendBack(in, sender);
    }
    
    /**
     *Checks if a packet is in range of this server or not.
     * @param p
     *     packet to be checked for range
     * @return
     *     returns true if the packet is in range or false if otherwise.
     */
    private boolean isRequestInRange(Packet p) {
        //Hashing the key and comparing it to the server's hashRange.
        int hash = hashit(p.key);
        int left = hashRange.left.intValue();
        int right = hashRange.right.intValue();
        if (left <= hash && hash <= right)
            return true;
        return false;
    }
    
    /**
     *Checks if a packet's key is in the cache, if the cache is set.
     * @param p
     *     packet to be checked.
     * @return
     *    returns true of the packet's key is in the cache and the
     *    cache is set, and false otherwise.
     */
    private boolean getFromCache(Packet p){
        if(cacheOn && cache.containsKey(p.key)){
            p.type = "success";
            p.val = cache.get(p.key);
            return true;
        }
        return false;
    }
    
    /**
     * Returns the packet to the address specified, usually the client.
     * @param in
     *     packet to be returned.
     * @param sender
     *     address for the packet to be returned.
     */
    private void sendBack(Packet in, InetSocketAddress sender) {
        //Initializing the reply address.
        InetSocketAddress replyAdr = null;
        
        //Setting the return address to the client, whether that is 
        //known in the packet or is the sender.
        if(equalInetSocketAddress(in.relayAdr, myAdr)
                || in.relayAdr == null){
            replyAdr = in.clientAdr == null ? sender : in.clientAdr ;
            in.cleanPacket();
        }
        
        //Returns to the relay address.
        else if (in.relayAdr != null) {
            in.senderInfo.left = myAdr;
            in.senderInfo.right = hashRange.left;
            replyAdr = in.relayAdr;
        }
        //return the packet.
        in.send(sock, replyAdr, debug);
        
    }
    
    /**
     * Forward a packet using the local routing table.
     *
     * @param p
     *            is a packet to be forwarded
     * @param sender
     *            the packet's sender address.
     *
     *          This method selects a server from its route table that is
     *          "closest" to the target of this packet (based on hash). If
     *          firstHash is the first hash in a server's range, then we
     *          seek to minimize the difference hash-firstHash, where the
     *          difference is interpreted modulo the range of hash values.
     *          IMPORTANT POINT - handle "wrap-around" correctly. Once a
     *          server is selected, p is sent to that server.
     */
    private void forward(Packet out, InetSocketAddress sender) {
        //Initializing the closestServer
        InetSocketAddress closestServer = null;
        
        //Setting the server's sender information.
        out.senderInfo = new Pair<InetSocketAddress, Integer>(myAdr,
                hashRange.left);
        
        //Set proper relay and client address.
        if (out.clientAdr == null) {
            out.relayAdr = myAdr;
            out.clientAdr = sender;
        }
        
        //Find the closest server
        closestServer = getClosestServer(hashit(out.key));
        // forward the packet.
        out.send(sock, closestServer, debug);
    }
    
    /**
     *Adds a packet's key and value if cache is on and the packet is
     * a success packet.
     * @param in
     *      the packet to be checked.
     */
    private void addToCache(Packet in){
        if(in.type.equals("success") && cacheOn){
            cache.put(in.key, in.val);
        }
    }
    
    /**
     * Handle a get packet.
     *
     * @param p
     *            is the packet with type set to get.
     */
    private void handleGet(Packet p) {
        //If the hashmap contains the key, set type to success and
        //fill in the corresponding value.
        if (map.containsKey(p.key)) {
            p.type = "success";
            p.val = map.get(p.key);
        }
        //otherwise, return no match.
        else
            p.type = "no match";
    }
    
    /**
     * Handle a put packet.
     *
     * @param p
     *             is the packet with type set to put.
     */
    private void handlePut(Packet p) {
        //If the put has no val clear the key.
        if (p.val == null)
            map.remove(p.key);
        //otherwise set it.
        else
            map.put(p.key, p.val);
        p.type = "success"; //indicate completion of command
    }
    
    /**
     * Handle a join packet from a prospective DHT node. This function
     * initializes the out packet as well as halves its hashRange and
     * sends it to the requesting server, as well as any data that the
     * new server might now be responsible for.
     *
     * @param p
     *            is the received join packet
     * @param succAdr
     *            is the socket address of the host that sent
     *            the join packet (the new successor)
     *
     */
    private void handleJoin(Packet out, InetSocketAddress succAdr) {
        //Clearing the incoming packet, initializingit to success
        //and no hashRange.
        out.clear();
        out.type = "success";
        out.hashRange = new Pair<Integer, Integer>(0, 0);
        
        //evaluating the appropriate hashRange difference and setting
        //it to the outgoing packet.
        int rangeDifference = hashRange.right.intValue()
                - hashRange.left.intValue();
        out.hashRange.right = hashRange.right.intValue();
        out.hashRange.left = hashRange.left.intValue()
                + (int) (rangeDifference / 2);
        
        
        //Setting the new hashRange maximum.
        hashRange.right = out.hashRange.left;
        
        //setting the successor the output packet.
        out.succInfo = new Pair<InetSocketAddress, Integer>
                                                                        (succInfo.left,succInfo.right.intValue());
        
        //Updating the server's own successor.
        succInfo.left = succAdr;
        succInfo.right = out.hashRange.left.intValue();
       	addRoute(succInfo); 
        //send the packet.
        out.send(sock, succAdr, debug);
        //transfer any data that the new server is now responsible for.
        transferData(succAdr);
    }
    
    /**
     * Transfers all the data the newly created server is responsible
     * for.
     * @param succAdr
     *       address of the newly created server.
     */
    private void transferData(InetSocketAddress succAdr) {
        //Create the outgong packet on which to send the pairs.
        Packet out = new Packet();
        @SuppressWarnings("rawtypes")
                Iterator it = map.entrySet().iterator();
        
        //Iterate over this server's hashmap/
        while (it.hasNext()) {
            @SuppressWarnings("rawtypes")
                    Map.Entry pair = (Map.Entry) it.next();
            out.key = (String) pair.getKey();
            out.val = (String) pair.getValue();
            
            //If the key,value pair is no longer in its range,
            //send it to the newly created server and remove it
            //from this server's hashmap.
            if (!isRequestInRange(out)) {
                out.type = "transfer";
                out.send(sock, succAdr, debug);
                it.remove();
            }
        }
    }
    
    /**
     * Handle a transfer packet.
     * Simply accept the packet's key value pair into the new server's
     * hashmap.
     * @param in
     *            is a transfer packet
     */
    public void handleXfer(Packet in) {
        map.put(in.key, in.val);
    }
    
    /**
     * Add an entry to the route table.
     * Adds a route or not depending on all the different sates that
     * the route table can be in.
     *
     * @param newRoute
     *         is a pair (addr,hash) where addr is the socket address for
     *         some server and hash is the first hash in that server's range
     *         If the number of entries in the table exceeds the max number
     *         allowed, the first entry that does not refer to the successor
     *         of this server, is removed. If debug is true and the set of
     *         stored routes does change, print the string "rteTbl=" +
     *         rteTbl. (IMPORTANT)
     */
    private void addRoute(Pair<InetSocketAddress, Integer> newRoute) {
        Pair<InetSocketAddress,Integer> myPair =
                new Pair<InetSocketAddress,Integer>(myAdr,hashRange.left);
        
        //Does not add null routes or this server itself to the table.
        if (newRoute == null || newRoute.equals(myPair))
            return;
        
        //Iterate over the routing table so that we do not add repeating
        //routes.
        for (Pair<InetSocketAddress, Integer> element : rteTbl) {
            if (element.equals(newRoute))
                return;
        }
        //Consider the cases when the size of the routing table is at
        //its limit.
        if (rteTbl.size() >= numRoutes ){
            if(rteTbl.size() == 1 && rteTbl.get(0).equals(succInfo))
                return;
            int rm_index = rteTbl.get(0).equals(succInfo)? 1:0;
            rteTbl.remove(rm_index);
        }
        //Add the new route.
        Pair<InetSocketAddress, Integer> routeToAdd = 
                new Pair<InetSocketAddress, Integer>(new InetSocketAddress(newRoute.left.getAddress(),newRoute.left.getPort()),newRoute.right.intValue());
        rteTbl.add(routeToAdd);
        //{Print the debug routing table.
        if (debug)
            System.out.println("rteTbl=" + rteTbl);
    }
    
    /**
     *Returns the address of the closest server of the given hash.
     * @param hash
     * @return
     *     address of the closest server to the hash.
     */
    private InetSocketAddress getClosestServer(int hash) {
        //Let the successor be the base case.
        int minimum = Math.abs(hash - succInfo.right);
        int difference;
        
        //Calculate the difference between each hash and each
        //element in the routing table.
        InetSocketAddress closestAddress = succInfo.left;
        for (Pair<InetSocketAddress, Integer> element : rteTbl) {
            difference = Math.abs(hash - element.right);
            
            //Choose a new min if it is closer to the hash and it does
            //not go beyond it.
            if (difference < minimum) {
                minimum = difference;
                closestAddress = element.left;
            }
        }
        return closestAddress;
    }
    
    /**
     *Compares the two input socket address and returns true if they
     * are the same and false otherwise.
     *
     * @param a
     *     The  first socket address to compare
     * @param b
     *     The second socket address to compare.
     * @return
     */
    private boolean equalInetSocketAddress(InetSocketAddress a,
            InetSocketAddress b) {
        if (a == null || b == null) return false;
        return a.getAddress().getHostAddress()
                .equals(b.getAddress().getHostAddress())
                && a.getPort() == b.getPort();
    }
    
    /**
     * Hash a string, returning a 32 bit integer.
     *
     * @param s
     *            is a string, typically the key from some
     *            get/put operation.
     * @return and integer hash value in the interval [0,2^31).
     */
    public int hashit(String s) {
        while (s.length() < 16)
            s += s;
        byte[] sbytes = null;
        try {
            sbytes = s.getBytes("US-ASCII");
        } catch (Exception e) {
            System.out.println("Illegal key string");
            System.exit(1);
        }
        int i = 0;
        int h = 0x37ace45d;
        while (i + 1 < sbytes.length) {
            int x = (sbytes[i] << 8) | sbytes[i + 1];
            h *= x;
            int top = h & 0xffff0000;
            int bot = h & 0xffff;
            h = top | (bot ^ ((top >> 16) & 0xffff));
            i += 2;
        }
        if (h < 0)
            h = -(h + 1);
        return h;
    }
}
