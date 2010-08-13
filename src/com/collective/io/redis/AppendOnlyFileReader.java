/* File:    AppendOnlyFileReader.java
 * Created: Jul 17, 2010
 * Author:  Lars George
 *
 * Copyright (c) 2010 larsgeorge.com
 */

package com.collective.io.redis;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.msgpack.Unpacker;


/**
 * Implements a Redis Append-Only-File (AOF) reader generated using the <code>BGREWRITEAOF</code> command sent to
 * Redis (see <a href="http://code.google.com/p/redis/wiki/BgrewriteaofCommand">command</a> and
 * <a href="http://code.google.com/p/redis/wiki/AppendOnlyFileHowto">reference</a> help online).
 *
 * @author Lars George
 */
public class AppendOnlyFileReader {

  static final Log LOG = LogFactory.getLog(AppendOnlyFileReader.class);

  private DataInputStream in = null;
  private long pos = 0;

  /**
   * Creates a new instance of this class. If the input stream is not already
   * a buffered one it wraps it in <code>BufferedInputStream</code> using the
   * default buffer size of that class.
   *
   * @param in  The input stream to use.
   */
  public AppendOnlyFileReader(InputStream in) {
    if (!(in instanceof BufferedInputStream))
      in = new BufferedInputStream(in);
    this.in = new DataInputStream(in);
  }

  /**
   * Creates a new instance of this class. If <code>bufferSize</code> is set to
   * "-1" is does not buffer at all. A value of "1" and greater will - if the
   * input is not already a buffered stream - cause the input stream to be
   * wrapped in <code>BufferedInputStream</code> configured with the given
   * buffer size.
   *
   * @param in  The input stream to use.
   * @param bufferSize  The specific buffer size to use.
   */
  public AppendOnlyFileReader(InputStream in, int bufferSize) {
    if (bufferSize > 0 && !(in instanceof BufferedInputStream)) {
      in = new BufferedInputStream(in, bufferSize);
      LOG.info("contructor: buffer size -> " + bufferSize);
    }
    this.in = new DataInputStream(in);
  }

  /**
   * Reads the next command from the input stream.
   *
   * @return The next command instance.
   * @throws IOException When reading from the stream fails.
   */
  public RedisCommand next() throws IOException {
    if (LOG.isDebugEnabled()) LOG.debug("pos: " + pos);
    // read start of record, assumes "*" + N + "\r\n", where N can be "3" or "4".
    String line = readLine(in);
    if (LOG.isDebugEnabled()) LOG.debug("line -> " + line);
    if (line == null || !line.startsWith("*")) return null;
    String cmd = readString(in);
    String key = readString(in);
    byte[] value = readBytes(in);
    RedisCommand.Type ct = RedisCommand.Type.valueOf(cmd);
    boolean isExpireAt = RedisCommand.Type.EXPIREAT == ct;
    return new RedisCommand(ct, key, isExpireAt ? null : value,
      isExpireAt ? Long.parseLong(new String(value, "ISO8859-1")) : -1L);
  }

  /**
   * Reads a length prefixed string.
   *
   * @param in  The input to read the characters from.
   * @return The string read from the stream.
   * @throws IOException  When reading from the stream fails.
   */
  private String readString(DataInputStream in) throws IOException {
    int len = readLength(in);
    byte[] raw = new byte[len];
    in.readFully(raw);
    // skip trailing "\r\n"
    in.read();
    in.read();
    String res = new String(raw, "ISO8859-1");
    if (LOG.isDebugEnabled()) LOG.debug("readString: res[" + (res != null ? res.length() : -1) + "] -> " + res);
    return res;
  }

  /**
   * Reads a length prefixed set of bytes.
   *
   * @param in  The input to read the characters from.
   * @return The bytes read from the stream.
   * @throws IOException  When reading from the stream fails.
   */
  private byte[] readBytes(DataInputStream in) throws IOException {
    int len = readLength(in);
    byte[] raw = new byte[len];
    in.readFully(raw);
    // skip trailing "\r\n"
    in.read();
    in.read();
    if (LOG.isDebugEnabled()) LOG.debug("readBytes: res -> " + new String(raw, "ISO8859-1"));
    return raw;
  }

  /**
   * Reads a length value from the AOF file. These are prefixed with a "$" sign.
   *
   * @param in  The input to read the characters from.
   * @return The value read from the stream.
   * @throws IOException  When reading from the stream fails.
   */
  private int readLength(DataInputStream in) throws IOException {
    String line = readLine(in);
    if (LOG.isDebugEnabled()) LOG.debug("readLength: line[" + (line != null ? line.length() : -1) + "] -> " + line);
    return Integer.parseInt(line.substring(1));
  }

  /**
   * Reads a line from the Redis AOF file. Assumes "\r\n" line endings (see redis.c#rewriteAppendFileOnly())
   * as well as ISO8859-1/ASCII encoding of characters.
   *
   * @param in  The input to read the characters from.
   * @return The line read from the stream.
   * @throws IOException  When reading from the stream fails.
   */
  private String readLine(DataInputStream in) throws IOException {
    StringBuilder res = new StringBuilder();
    int c = in.read();
    boolean done = false;
    while (c > -1 && !done) {
      int c2 = -1;
      // if we see a carriage return read another to check for line feed
      if (c == '\r') c2 = in.read();
      // we are done if we find those two
      done = c == '\r' && c2 == '\n';
      // otherwise add character to buffer
      if (!done) {
        res.append((char) c);
        // if we did read ahead or are at EOF use c2, else read anew
        c = c2 > -1 || c == -1 ? c2 : in.read();
      }
    }
    return res.toString();
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
   * Test harness.
   *
   * @param args  The command line parameters.
   */
  public static void main(String[] args) {
    try {
      FileInputStream fis = new FileInputStream(args[0]);
      AppendOnlyFileReader r = new AppendOnlyFileReader(fis);
      long time = System.currentTimeMillis();
      int max = Integer.MAX_VALUE;
      if (args.length > 1) max = Integer.parseInt(args[1]);
      int i = 0;
      for (; i < max && r.hasNext(); i++) {
        RedisCommand c = r.next();
        System.out.println("command[" + i + "]: " + c);
        if (c != null) {
          ByteArrayInputStream bais = new ByteArrayInputStream(c.getValue());
          Unpacker up = new Unpacker(bais);
          for (Object o : up) {
            System.out.println("printing object...");
            printObject(o, "");
            System.out.println("to json -> " + toJSON(o));
          }
        }
        if (i > 600) break;
      }
      System.out.println("count: " + i);
      System.out.println("time: " + (System.currentTimeMillis() - time) + "ms.");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static Object toJSON(Object o) throws UnsupportedEncodingException, JSONException {
    if (o instanceof Map) {
      JSONObject jmap = new JSONObject();
      for (Iterator iter = ((Map) o).keySet().iterator(); iter.hasNext(); ) {
        Object key = iter.next();
        Object val = ((Map) o).get(key);
        jmap.put(toJSON(key).toString(), toJSON(val));
      }
      return jmap;
    } else if (o instanceof List) {
      JSONArray jarr = new JSONArray();
      for (Iterator iter = ((List) o).iterator(); iter.hasNext(); ) {
        Object li = iter.next();
        jarr.put(toJSON(li));
      }
      return jarr;
    } else if (o instanceof byte[]) {
      return new String((byte[]) o, "UTF-8");
    } else {
      return o.toString();
    }
  }

  private static void printObject(Object o, String indent) throws UnsupportedEncodingException {
    if (o instanceof Map) {
      System.out.println(indent  + "map -> ");
      boolean hadOne = false;
      for (Iterator iter = ((Map) o).keySet().iterator(); iter.hasNext(); ) {
        hadOne = true;
        Object key = iter.next();
        System.out.print(indent  + "  key -> ");
        printObject(key, indent + "    ");
        System.out.print(indent  + "  value -> ");
        printObject(((Map) o).get(key), indent + "    ");
      }
      if (!hadOne) System.out.println("");
    } else if (o instanceof byte[]) {
      System.out.println(indent  + "byte[] -> " + new String((byte[]) o, "UTF-8"));
    } else if (o instanceof Byte) {
      System.out.println(indent  + "Byte -> " + o);
    } else if (o instanceof Short) {
      System.out.println(indent  + "Short -> " + o);
    } else if (o instanceof Integer) {
      System.out.println(indent  + "Integer -> " + o);
    } else if (o instanceof Long) {
      System.out.println(indent  + "Long -> " + o);
    } else if (o instanceof List) {
      System.out.print(indent  + "List -> ");
      boolean hadOne = false;
      for (Iterator iter = ((List) o).iterator(); iter.hasNext(); ) {
        hadOne = true;
        Object li = iter.next();
        printObject(li, "    ");
      }
      if (!hadOne) System.out.println("");
    } else {
      System.err.println("Unhandled type -> " + o.getClass().getName());
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
   * @throws java.io.IOException is thrown if anything goes wrong writing the data to
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
