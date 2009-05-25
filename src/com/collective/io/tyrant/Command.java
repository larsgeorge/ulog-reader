/* File:    Command.java
 * Created: May 14, 2009
 * Author:  Lars George
 * 
  * Copyright (c) 2009 larsgeorge.com
*/

package com.collective.io.tyrant;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wraps each command into a handy class.
 */
public class Command {

  static final Log LOG = LogFactory.getLog(Command.class);

  public final static int TTMAGICNUM     = 0xc8; /* magic number of each command */
  public final static int TTCMDPUT       = 0x10; /* ID of put command */
  public final static int TTCMDPUTKEEP   = 0x11; /* ID of putkeep command */
  public final static int TTCMDPUTCAT    = 0x12; /* ID of putcat command */
  public final static int TTCMDPUTSHL    = 0x13; /* ID of putshl command */
  public final static int TTCMDPUTNR     = 0x18; /* ID of putnr command */
  public final static int TTCMDOUT       = 0x20; /* ID of out command */
  public final static int TTCMDGET       = 0x30; /* ID of get command */
  public final static int TTCMDMGET      = 0x31; /* ID of mget command */
  public final static int TTCMDVSIZ      = 0x38; /* ID of vsiz command */
  public final static int TTCMDITERINIT  = 0x50; /* ID of iterinit command */
  public final static int TTCMDITERNEXT  = 0x51; /* ID of iternext command */
  public final static int TTCMDFWMKEYS   = 0x58; /* ID of fwmkeys command */
  public final static int TTCMDADDINT    = 0x60; /* ID of addint command */
  public final static int TTCMDADDDOUBLE = 0x61; /* ID of adddouble command */
  public final static int TTCMDEXT       = 0x68; /* ID of ext command */
  public final static int TTCMDSYNC      = 0x70; /* ID of sync command */
  public final static int TTCMDOPTIMIZE  = 0x71; /* ID of optimize command */
  public final static int TTCMDVANISH    = 0x72; /* ID of vanish command */
  public final static int TTCMDCOPY      = 0x73; /* ID of copy command */
  public final static int TTCMDRESTORE   = 0x74; /* ID of restore command */
  public final static int TTCMDSETMST    = 0x78; /* ID of setmst command */
  public final static int TTCMDRNUM      = 0x80; /* ID of rnum command */
  public final static int TTCMDSIZE      = 0x81; /* ID of size command */
  public final static int TTCMDSTAT      = 0x88; /* ID of stat command */
  public final static int TTCMDMISC      = 0x90; /* ID of misc command */
  public final static int TTCMDREPL      = 0xa0; /* ID of repl command */

  private int command = -1;
  private long timestamp = -1;
  private int sid = -1;
  private String name = null;
  private int size = -1;
  private Map<String, String> elements = null;
  
  /**
   * Parses the given data.
   * 
   * @param data  The data in its raw format.
   * @param sid  The session ID.
   * @param timestamp  The timestamp when the event happened.
   * @param size  Total size of the command including headers.
   * @throws IOException When reading the data fails.
   */
  public Command(byte[] data, long timestamp, int sid, int size) throws IOException {
    this.timestamp = timestamp;
    this.sid = sid;
    this.setSize(size);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
    int magic = in.readUnsignedByte();
    if (magic != TTMAGICNUM) throw new IOException("Wrong magic header.");
    command = in.readUnsignedByte();
    switch (command) {
      case TTCMDMISC:
        parseCommandMisc(in);
        break;
      case TTCMDPUT:
        name = "put";
        parseCommandPut(in);
        break;
      case TTCMDPUTKEEP:
        name = "putkeep";
        parseCommandPut(in);
        break;
      default:
        if (LOG.isDebugEnabled()) LOG.debug("constructor: Found unhandled command -> 0x" + Integer.toHexString(command));
        break;
    }
    in.close();
    in = null;
  }

  /**
   * Parses the miscellaneous command.
   * 
   * @param in  The stream to read from.
   * @throws IOException  When reading the data fails.
   */
  private void parseCommandMisc(DataInputStream in) throws IOException {
    int nsize = in.readInt();
    int lsize = in.readInt();
    byte[] bname = new byte[nsize];
    in.read(bname);
    name = new String(bname);
    if (LOG.isDebugEnabled()) LOG.debug("parseCommandMisc: name -> " + name);
    elements = new LinkedHashMap<String, String>(lsize / 2);
    // iterate over list elements
    for (int i = 0; i < lsize / 2; i++) {
      int esize = in.readInt();
      byte[] key = new byte[esize];
      in.read(key);
      esize = in.readInt();
      byte[] value = new byte[esize];
      in.read(value);
      elements.put(new String(key), new String(value));
    }
    if (LOG.isDebugEnabled()) LOG.debug("parseCommandMisc: elements.size -> " + elements.size());
    // int rv = 
    in.readUnsignedByte(); // unused
  }

  /**
   * Parses the put/putkeep command.
   * 
   * @param in  The stream to read from.
   * @throws IOException  When reading the data fails.
   */
  private void parseCommandPut(DataInputStream in) throws IOException {
    int ksize = in.readInt();
    int vsize = in.readInt();
    byte[] key = new byte[ksize];
    in.read(key);
    byte[] value = new byte[vsize];
    in.read(value);
    // int err = 
    in.readUnsignedByte(); // unused
    elements = new LinkedHashMap<String, String>(1);
    elements.put(new String(key), new String(value));
    if (LOG.isDebugEnabled()) LOG.debug("parseCommandPut: elements -> " + elements);
  }

  /**
   * @return Returns the command.
   */
  public int getCommand() {
    return command;
  }

  /**
   * @param command The command to set.
   */
  public void setCommand(int command) {
    this.command = command;
  }

  /**
   * @return Returns the timestamp.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * @param timestamp The timestamp to set.
   */
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  /**
   * @return Returns the sid.
   */
  public int getSid() {
    return sid;
  }

  /**
   * @param sid The sid to set.
   */
  public void setSid(int sid) {
    this.sid = sid;
  }

  /**
   * @param size The size to set.
   */
  public void setSize(int size) {
    this.size = size;
  }

  /**
   * @return Returns the size.
   */
  public int getSize() {
    return size;
  }

  /**
   * @return Returns the name.
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set.
   */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * @return Returns the elements.
   */
  public Map<String, String> getElements() {
    return elements;
  }

  /**
   * @param elements The elements to set.
   */
  public void setElements(Map<String, String> elements) {
    this.elements = elements;
  }

}

