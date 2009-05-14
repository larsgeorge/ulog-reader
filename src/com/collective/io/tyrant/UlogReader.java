/* File:    UlogReader.java
 * Created: May 13, 2009
 * Author:  Lars George
 */

package com.collective.io.tyrant;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

//==============================================================================

/**
 * Implements a update log (ulog) reader for Tokyo Tyrant and Tokyo Cabinet.
 * 
 * @author Lars George
 */
public class UlogReader {

  private final static int TCULMAGICNUM   = 0xc9; /* magic number of each command */

  //----------------------------------------------------------------------------
  
  private DataInputStream in = null;
  
  //----------------------------------------------------------------------------

  public UlogReader(InputStream in) {
    if (!(in instanceof BufferedInputStream))
      in = new BufferedInputStream(in);
    this.in = new DataInputStream(in);
  }
  
  public Command next() throws IOException {
    int magic = in.readUnsignedByte();
    if (magic != TCULMAGICNUM) throw new IOException("Wrong magic header.");
    long timestamp = in.readLong();
    System.out.println("ts: " + timestamp + ", date: " + new Date(timestamp / 1000));
    int sid = in.readInt();
    System.out.println("sid: " + sid);
    int size = in.readInt();
    System.out.println("size: " + size);
    byte[] res = new byte[size];
    in.readFully(res);
    return new Command(res);
  }
  
  public boolean hasNext() throws IOException {
    return in.available() > 0;
  }
  
  //----------------------------------------------------------------------------
  
  /**
   * Test harness.
   * 
   * @param args  The command line parameters.
   */
  public static void main(String[] args) {
    try {
      FileInputStream fis = new FileInputStream("C:\\Documents and Settings\\Lars George\\My Documents\\Downloads\\small-ulog.bin");
      UlogReader r = new UlogReader(fis);
      while (r.hasNext()) {
        Command c = r.next();
        System.out.println("elements: " + c.getElements());
      }
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }

}
