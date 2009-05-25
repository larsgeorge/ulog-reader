/* File:    UlogReader.java
 * Created: May 13, 2009
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.collective.io.tyrant;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implements a update log (ulog) reader for Tokyo Tyrant and Tokyo Cabinet.
 * 
 * @author Lars George
 */
public class UlogReader {

  static final Log LOG = LogFactory.getLog(UlogReader.class);

  /** The size of the headers up to the command. */
  public static final int SIZE_HEADER = 1 + 8 + 4 + 4 + 1;
  /** Magic number of each command. */
  public static final int TCULMAGICNUM   = 0xc9; 
      

  private DataInputStream in = null;
  private long pos = 0;
  
  /**
   * Creates a new instance of this class.
   * 
   * @param in  The input stream to use.
   */
  public UlogReader(InputStream in) {
    if (!(in instanceof BufferedInputStream))
      in = new BufferedInputStream(in);
    this.in = new DataInputStream(in);
  }
  
  /**
   * Reads the next command from the input stream.
   * 
   * @return The next command instance.
   * @throws IOException When reading from the stream fails.
   */
  public Command next() throws IOException {
    System.out.println("pos: " + pos);
    int magic = in.readUnsignedByte();
    pos += 1;
    if (magic != TCULMAGICNUM) throw new IOException("Wrong magic header.");
    long timestamp = in.readLong();
    pos += 8;
    if (LOG.isDebugEnabled()) LOG.debug("next: ts -> " + timestamp + ", date -> " + new Date(timestamp / 1000));
    int sid = in.readInt();
    pos += 4;
    if (LOG.isDebugEnabled()) LOG.debug("next: sid -> " + sid);
    int size = in.readInt();
    pos += 4;
    if (LOG.isDebugEnabled()) LOG.debug("next: size -> " + size);
    byte[] res = new byte[size];
    in.readFully(res);
    pos += size;
    return new Command(res, timestamp, sid, size + 1 + 8 + 4 + 4);
  }
  
  /**
   * Reads the next command from the given buffer.
   * 
   * @param buffer  The buffer to read from.
   * @return The next command instance.
   * @throws IOException When the buffer is faulty.
   */
  public static Command next(byte[] buffer, boolean checkMagic) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    if (checkMagic) {
      int magic = dis.readUnsignedByte();
      if (magic != TCULMAGICNUM) throw new IOException("Wrong magic header.");
    }
    long timestamp = dis.readLong();
    if (LOG.isDebugEnabled()) LOG.debug("next: ts -> " + timestamp + ", date -> " + new Date(timestamp / 1000));
    int sid = dis.readInt();
    if (LOG.isDebugEnabled()) LOG.debug("next: sid -> " + sid);
    int size = dis.readInt();
    if (LOG.isDebugEnabled()) LOG.debug("next: size -> " + size);
    byte[] res = new byte[size];
    dis.readFully(res);
    return new Command(res, timestamp, sid, size + 1 + 8 + 4 + 4);
  }
  
  /**
   * Checks if another command may be available.
   * 
   * @return <code>true</code> if another command may be available.
   * @throws IOException When the input stream is faulty.
   */
  public boolean hasNext() throws IOException {
    return in.available() > 0;
  }
  
  /**
   * Verifies if the given byte array is a valid header of a command object.
   * 
   * @param header  The header array to check.
   * @return <code>true</code> when this array is a header.
   */
  public static boolean isValidHeader(byte[] header) {
    return header.length >= SIZE_HEADER && 
      ((int) header[0] & 0xff) == TCULMAGICNUM && 
      ((int) header[SIZE_HEADER - 1] & 0xff) == Command.TTMAGICNUM;
  }
  
  /**
   * Test harness.
   * 
   * @param args  The command line parameters.
   */
  public static void main(String[] args) {
    try {
      FileInputStream fis = new FileInputStream(args[0]);
      UlogReader r = new UlogReader(fis);
      while (r.hasNext()) {
        Command c = r.next();
        System.out.println("elements: " + c.getElements().toString().substring(0, 100));
      }
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }

  // DEBUG HELPER - from org.apache.commons.io.HexDump
  
  /**
   * Dump an array of bytes to an OutputStream.
   * 
   * @param data the byte array to be dumped
   * @param offset its offset, whatever that might mean
   * @param stream the OutputStream to which the data is to be written
   * @param index initial index into the byte array
   * 
   * @throws IOException is thrown if anything goes wrong writing the data to
   *         stream
   * @throws ArrayIndexOutOfBoundsException if the index is outside the data
   *         array's bounds
   * @throws IllegalArgumentException if the output stream is null
   */
  public static void dump(byte[] data, long offset, OutputStream stream,
      int index, int lines) throws IOException, ArrayIndexOutOfBoundsException,
      IllegalArgumentException {

    if ((index < 0) || (index >= data.length)) {
      throw new ArrayIndexOutOfBoundsException("illegal index: " + index
          + " into array of length " + data.length);
    }
    if (stream == null) {
      throw new IllegalArgumentException("cannot write to nullstream");
    }
    long display_offset = offset + index;
    StringBuffer buffer = new StringBuffer(74);
    int n = 0;
    for (int j = index; j < data.length; j += 16) {
      int chars_read = data.length - j;

      if (chars_read > 16) {
        chars_read = 16;
      }
      dump(buffer, display_offset).append(' ');
      for (int k = 0; k < 16; k++) {
        if (k < chars_read) {
          dump(buffer, data[k + j]);
        } else {
          buffer.append("  ");
        }
        buffer.append(' ');
      }
      for (int k = 0; k < chars_read; k++) {
        if ((data[k + j] >= ' ') && (data[k + j] < 127)) {
          buffer.append((char) data[k + j]);
        } else {
          buffer.append('.');
        }
      }
      buffer.append(EOL);
      stream.write(buffer.toString().getBytes());
      stream.flush();
      buffer.setLength(0);
      display_offset += chars_read;
      if (n++ == lines) break;
    }
  }

  public static final String EOL = System.getProperty("line.separator");
  private static final char[] _hexcodes = { '0', '1', '2', '3', '4', '5', '6',
      '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
  private static final int[] _shifts = { 28, 24, 20, 16, 12, 8, 4, 0 };

  /**
   * Dump a long value into a StringBuffer.
   * 
   * @param _lbuffer the StringBuffer to dump the value in
   * @param value the long value to be dumped
   * @return StringBuffer containing the dumped value.
   */
  private static StringBuffer dump(StringBuffer _lbuffer, long value) {
    for (int j = 0; j < 8; j++) {
      _lbuffer.append(_hexcodes[((int) (value >> _shifts[j])) & 15]);
    }
    return _lbuffer;
  }

  /**
   * Dump a byte value into a StringBuffer.
   * 
   * @param _cbuffer the StringBuffer to dump the value in
   * @param value the byte value to be dumped
   * @return StringBuffer containing the dumped value.
   */
  private static StringBuffer dump(StringBuffer _cbuffer, byte value) {
    for (int j = 0; j < 2; j++) {
      _cbuffer.append(_hexcodes[(value >> _shifts[j + 6]) & 15]);
    }
    return _cbuffer;
  }

}
