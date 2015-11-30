
/**
 * Authors: Carlos Gonzalez, Nicola Pedretti
 * Washington University in St. Louis
 * CSE 473: Introduction to Computer Networks
 * Lab 3
 *
 * The Packet class creates a very useful object to which we can set a
 * wide variety of properties which will allow for easy communication
 * between the DhtServer and the DhtClient.
 **/

import java.net.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Class for working with DHT packets. */
public class Packet {
    
    // packet fields - note: all are public
    public String type; // packet type
    public int ttl; // time-to-live
    public String key; // DHT key string
    public String val; // DHT value string
    public String reason; // reason for a failure
    public InetSocketAddress clientAdr; // address of original client
    public InetSocketAddress relayAdr; // address of first DHT server
    public int tag; // tag used to identify packet
    public Pair<Integer, Integer> hashRange; // range of hash values
    public Pair<InetSocketAddress, Integer> senderInfo;//address, first hash
    public Pair<InetSocketAddress, Integer> succInfo; //address, first hash
    
    /** Constructor, initializes fields to default values. */
    public Packet() {
        clear();
    }
    
    /**
     * Initialize all packet fields. Initializes all fields with a 
     * standard initial value or makes them undefined.
     */
    public void clear() {
        type = null;
        ttl = 100;
        key = null;
        val = null;
        reason = null;
        clientAdr = null;
        relayAdr = null;
        tag = -1;
        hashRange = null;
        senderInfo = null;
        succInfo = null;
    }
    
    /**
     * Pack attributes defining packet fields into buffer. 
     * Fails if the packet type is undefined or if the resulting 
     * buffer exceeds the allowed length of 1400 bytes.
     *
     * @return null on failure, otherwise a byte array 
     *         containing the packet payload.
     */
    public byte[] pack() {
        if (type == null)
            return null;
        byte[] buf;
        try {
            buf = toString().getBytes("US-ASCII");
        } catch (Exception e) {
            return null;
        }
        if (buf.length > 1400)
            return null;
        return buf;
    }
    
    /**
     * Basic validity checking for received packets.
     *
     * @return true on success, false on failure; on failure, place an
     *         explanatory String in the reason field of the packet
     */
    public boolean check() {
        switch (type) {
            case "put":
            case "get":
                if (key == null) {
                    reason = "gets and puts require key and tag";
                    return false;
                }
            case "success":
            case "failure":
            case "no match":
            case "transfer":
            case "join":
                break;
            default:
                reason = "unrecognizable input";
                return false;
        }
        return true;
    }
    
    /**
     * Unpack attributes defining packet fields from buffer.
     *
     * @param buf
     *            is a byte array containing the DHT packet 
     *            (or if you like, the payload of a UDP packet).
     * @param bufLen
     *            is the number of valid bytes in buf
     */
    public boolean unpack(byte[] buf, int bufLen) {
        String packet;
        try {
            packet = new String(buf, 0, bufLen, "US-ASCII");
        } catch (Exception e) {
            return false;
        }
        String[] lines = packet.split("\n");
        for (String line : lines) {
            ArrayList<String> input = parseInput(line);
            if (input == null)
                return false;
            assignValue(input);
        }
        return true;
    }
    
    /**
     * parseInput allows for easy recognition of the commands
     * input in the command line, and their matchign property in
     * the packet object.
     *
     * @param input
     *        input to be parsed
     * @return
     */
    private ArrayList<String> parseInput(String input) {
        
        ArrayList<String> matches = new ArrayList<String>();
        //Identify the required inputs by using regex and
        //iterating over the entire input.
        String[] regExs = { "(CSE473 DHTPv0.2015)",
            "(type|ttl|key|val|reason|tag):(.+)",
            "(clientAdr|relayAdr|hashRange):([^:]+):([^:]+)",
            "(succInfo|senderInfo):([^:]+):([^:]+):([^:]+)" };
        for (int numGroups = 0; numGroups < regExs.length; numGroups++) {
            Pattern pattern = Pattern.compile(regExs[numGroups]);
            Matcher match = pattern.matcher(input);
            int group = 0;
            if (match.matches()) {
                while (++group > 0) {
                    try {
                        matches.add(match.group(group));
                    } catch (Exception e) {
                        break;
                    }
                }
            }
        }
        return matches.size() > 0 ? matches : null;
    }
    
    /**
     * Given an aArrayList of Strings, it parses each of its
     * elements and inputs them in the appropriate Packet parameter.
     */
    private void assignValue(ArrayList<String> line) {
        switch (line.get(0)) {
            case "CSE473 DHTPv0.2015":
                break;
            case "type":
                type = line.size()< 1? null : line.get(1);
                break;
            case "key":
                key = line.size()< 1? null :line.get(1);
                break;
            case "ttl":
                ttl = line.size()< 1? null : Integer.parseInt(line.get(1));
                break;
            case "val":
                val = line.size()< 1? null :line.get(1);
                break;
            case "reason":
                reason = line.size()< 1? null :line.get(1);
                break;
            case "tag":
                tag = line.size()< 1? null :Integer.parseInt(line.get(1));
                break;
            case "relayAdr":
                relayAdr = new InetSocketAddress(line.get(1),
                        Integer.parseInt(line.get(2)));
                break;
            case "clientAdr":
                clientAdr = new InetSocketAddress(line.get(1),
                        Integer.parseInt(line.get(2)));
                break;
            case "succInfo":
                succInfo = new Pair<InetSocketAddress, Integer>(
                        new InetSocketAddress(line.get(1),
                                Integer.parseInt(line
                                .get(2))), Integer.parseInt(line.get(3)));
                break;
            case "senderInfo":
                senderInfo = new Pair<InetSocketAddress, Integer>(
                        new InetSocketAddress(line.get(1), 
                                Integer.parseInt(line
                                .get(2))), Integer.parseInt(line.get(3)));
                break;
            case "hashRange":
                hashRange = new Pair<Integer, Integer>(
                        Integer.parseInt(line.get(1)),
                        Integer.parseInt(line.get(2)));
                break;
        }
    }
    
    /**
     * Create String representation of packet. The resulting
     * String is produced using the defined attributes and is
     * formatted with one field per line, allowing it to be used
     * as the actual buffer contents.
     */
    public String toString() {
        StringBuffer s = new StringBuffer("CSE473 DHTPv0.2015\n");
        if (type != null) {
            s.append("type:" + type + "\n");
        }
        if (key != null) {
            s.append("key:" + key + "\n");
        }
        if (relayAdr != null) {
            s.append("relayAdr:" + relayAdr.getAddress().getHostAddress()
                    + ":" + relayAdr.getPort() + "\n");
        }
        if (hashRange != null) {
            s.append("hashRange:" + hashRange.left + ":" + hashRange.right
                    + "\n");
        }
        if (senderInfo != null) {
            s.append("senderInfo:"
                    + senderInfo.left.getAddress().getHostAddress()
                    + ":" + senderInfo.left.getPort() + ":" 
                    + senderInfo.right + "\n");
        }
        if (val != null) {
            s.append("val:" + val + "\n");
        }
        if (reason != null) {
            s.append("reason:" + reason + "\n");
        }
        if (clientAdr != null) {
            s.append("clientAdr:" 
                    + clientAdr.getAddress().getHostAddress()
                    + ":" + clientAdr.getPort() + "\n");
        }
        if (tag != -1) {
            s.append("tag:" + tag + "\n");
        }
        if (succInfo != null) {
            s.append("succInfo:" 
                    + succInfo.left.getAddress().getHostAddress()
                    + ":" + succInfo.left.getPort() + ":" 
                    + succInfo.right + "\n");
        }
        if (ttl != -1) {
            s.append("ttl:" + ttl + "\n");
        }
        return s.toString();
    }
    
    /**
     * Send the packet to a specified destination. Packs the various packet
     * fields into a buffer before sending. Does no validity checking.
     *
     * @param sock
     *            is the socket on which the packet is sent
     * @param dest
     *            is the socket address of the destination debug is a flag; 
     *            if true, the packet is printed before it is sent
     * @return true on success, false on failure
     */
    public boolean send(DatagramSocket sock, InetSocketAddress dest,
            boolean debug) {
        if (debug) {
            System.out.println("" + sock.getLocalSocketAddress()
                    + " sending packet to " + dest + "\n" + toString());
            System.out.flush();
        }
        byte[] buf = pack();
        if (buf == null)
            return false;
        DatagramPacket pkt = new DatagramPacket(buf, buf.length);
        pkt.setSocketAddress(dest);
        try {
            sock.send(pkt);
        } catch (Exception e) {
            return false;
        }
        return true;
    }
    
    /**
     * Cleans the packet before sending it to the client by setting 
     * the unnecessary parameters to null. 
     */
    public void cleanPacket() {
        clientAdr = null;
        senderInfo = null;
        relayAdr = null;
    }
    
    /**
     * Get the next packet on the socket.
     *
     * Receives the next datagram from the socket and unpacks it.
     *
     * @param sock
     *            is the socket on which the packet is received
     * @param debug
     *            is a flag; if it is true, the received packet is printed
     * @return the sender's socket address on success and null on failure
     */
    public InetSocketAddress receive(DatagramSocket sock, boolean debug) {
        clear();
        byte[] buf = new byte[2000];
        DatagramPacket pkt = new DatagramPacket(buf, buf.length);
        try {
            sock.receive(pkt);
        } catch (Exception e) {
            System.out.println("Receive exception: " + e);
            return null;
        }
        
        if (!unpack(buf, pkt.getLength())) {
            System.out.println("Error while unpacking packet");
            return null;
        }
        ttl--;
        if (debug) {
            System.out.println(sock.getLocalSocketAddress()
                    + " received packet from " + pkt.getSocketAddress() 
                    + "\n" + toString());
            System.out.flush();
        }
        if (ttl < 0)
            return null;
        return (InetSocketAddress) pkt.getSocketAddress();
    }
}