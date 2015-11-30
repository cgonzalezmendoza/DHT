
/**
 * Authors: Carlos Gonzalez, Nicola Pedretti
 * Washington University in St. Louis
 * CSE 473: Introduction to Computer Networks
 * Lab 3
 *
 * The generics class creates practical functions to be used by both
 * DhtServer and DhtCLient.
 **/

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
public class Generics {
    
    //Flags for each possible error.
    public final int WRONG_ARGUMENTS = 1;
    public final int PACKET_FAILURE = 2;
    public final int READ_FILE_ERROR = 3;
    public final int WRITE_FILE_ERROR = 4;
    public final int ERROR_JOINING = 5;
    
    /**
     * Error checking function for input parameters.
     * @param required
     *      Represents a required string
     * @param optional
     *      Represents an optional string
     * @param critical
     *      Critical error that closes the server if set.
     */
    public void usage(String[] required, String[] optional, boolean critical){
        System.out.print("usage:");
        for (String req : required) {
            System.out.print(" " + req + " ");
        }
        for (String opt : optional) {
            System.out.print(" [" + opt + "]");
        }
        if (critical) {
            System.exit(WRONG_ARGUMENTS);
        }
    }
    
    /**
     *Reads a file and outputs it unto an ArrayList.
     * @param fName
     *       Name of the file
     * @return
     *       ArrayList of the text on a file.
     */
    public ArrayList<String> readLinesFromFile(String fName) {
        ArrayList<String> serverInfo = new ArrayList<String>();
       
        try {
            BufferedReader br = new BufferedReader(new FileReader(fName));
            String line;
            while ((line = br.readLine()) != null) {
                if(!line.trim().isEmpty())serverInfo.add(line);
            }
        } catch (Exception e) {
            System.out.println("Error reading from server file.");
            System.exit(READ_FILE_ERROR);
        }
        return serverInfo;
        
    }
    
    /**
     *Writes lines to a specific file
     * @param lines
     *     Lines to be written
     * @param fName
     *     Name of the file on which to write the lines on.
     */
    public void writeLinesToFile(ArrayList<String> lines, String fName) {
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(fName), "US-ASCII"));
            for (String line : lines) {
                bw.write(line);
                bw.newLine();
            }
            bw.close();
        } catch (Exception e) {
            System.out.println("Error writing to server file.");
            System.exit(WRITE_FILE_ERROR);
        }
        
    }
}
