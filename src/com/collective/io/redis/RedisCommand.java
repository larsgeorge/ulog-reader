/* File:    Command.java
 * Created: Jul 17, 2010
 * Author:  Lars George
 *
 * Copyright (c) 2010 larsgeorge.com
*/

package com.collective.io.redis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wraps each command into a convenience class.
 */
public class RedisCommand {

  static final Log LOG = LogFactory.getLog(RedisCommand.class);

  public static enum Type { SELECT, SET, RPUSH, SADD, ZADD, HSET, EXPIREAT }

  private Type command = null;
  private String key = null;
  private byte[] value = null;
  private long expire = -1;

  /**
   * Parses the given data.
   *
   * @param command  The command represented.
   * @param key  The key of the command.
   * @param expire  The expiry time for the command (optional).
   */
  public RedisCommand(Type command, String key, byte[] value, long expire) {
    this.command = command;
    this.key = key;
    this.value = value;
    this.expire = expire;
  }

  public Type getCommand() {
    return command;
  }

  public void setCommand(Type command) {
    this.command = command;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  public long getExpire() {
    return expire;
  }

  public void setExpire(long expire) {
    this.expire = expire;
  }

  /**
   * Dump instance details.
   *
   * @return The values of this instance as a string.
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "command -> " + command + ", key -> " + key + ", value -> " + value + ", expireAt -> " + expire;
  }

}

