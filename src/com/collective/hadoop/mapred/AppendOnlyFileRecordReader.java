/* File:    UlogRecordReader.java
 * Created: May 22, 2009
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.collective.hadoop.mapred;

// Java libs
import java.io.IOException;
import java.io.InputStream;
// Apache libs
import com.collective.io.redis.RedisCommand;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
// Collective libs
import com.collective.io.redis.AppendOnlyFileReader;

/**
 * Reads the Redis AOF binary files.
 *
 * @author Lars George
 */
public class AppendOnlyFileRecordReader extends RecordReader<LongWritable, RedisCommand> {

  static final Log LOG = LogFactory.getLog(AppendOnlyFileRecordReader.class);

  /** Can be used to set the buffer during reads. */
  public static final String BUFFER_SIZE         = "AppendOnlyFileRecordReader.bufferSize";

  private long start;
  private long pos;
  private long end;
  private boolean stillInChunk = true;
  private FSDataInputStream in = null;
  private AppendOnlyFileReader reader = null;
  private CompressionCodecFactory compressionCodecs = null;
  private LongWritable key = null;
  private RedisCommand command = null;
  private Path inputFile = null;

  /**
   * Initializes the reader.
   *
   * @param genericSplit  The current split.
   * @param context  The job context.
   * @throws java.io.IOException When the split is faulty.
   * @throws InterruptedException When the job is interrupted.
   * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
   *   org.apache.hadoop.mapreduce.InputSplit,
   *   org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context)
  throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    FileSplit split = (FileSplit) genericSplit;
    // get start and end details
    start = split.getStart();
    end = start + split.getLength();
    pos = start;
    // open input stream
    inputFile = split.getPath();
    LOG.info("initialize: split -> " + split);
    compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
    CompressionCodec codec = compressionCodecs.getCodec(inputFile);
    FileSystem fs = inputFile.getFileSystem(conf);
    in = fs.open(inputFile);
    // skip to the start offset
    if (start != 0L) in.seek(start);
    if (LOG.isDebugEnabled()) LOG.debug("initialize: start -> " + start + ", end -> " + end);
    if (LOG.isDebugEnabled()) LOG.debug("initialize: skip to pos -> " + pos);
    if (LOG.isDebugEnabled()) LOG.debug("initialize: stream pos -> " + in.getPos());
    // optionally wrap with decoder
    InputStream is = codec != null ? codec.createInputStream(in) : in;
    // check if we should use a specific buffer size
    String buffSize = conf.get(BUFFER_SIZE);
    reader = buffSize == null ? new AppendOnlyFileReader(is) :
      new AppendOnlyFileReader(is, Integer.parseInt(buffSize));
  }

  /**
   * Returns the current processed file name. For debugging mainly.
   *
   * @return Returns the input file name.
   */
  public Path getInputFile() {
    return inputFile;
  }

  /**
   * Changes reading from a single line to multiple ones. Handles the problem
   * of reading beyond the chunk.
   *
   * @return <code>true</code> when data was read.
   * @throws java.io.IOException When reading the data fails.
   * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
   */
  public boolean nextKeyValue() throws IOException {
    if (!stillInChunk) return false;
    command = reader.next();
    // check for a "null" record at a split start
    if (command == null) command = reader.next();
    if (key == null) key = new LongWritable();
    key.set(in.getPos());
    if (LOG.isDebugEnabled()) LOG.debug("nextKeyValue: command -> " + (command != null ? command.getCommand() : null));
    if (in.getPos() >= end) stillInChunk = false;
    return true;
  }

  /**
   * Closes the input.
   *
   * @throws java.io.IOException When closing the input fails.
   * @see org.apache.hadoop.mapreduce.RecordReader#close()
   */
  @Override
  public void close() throws IOException {
    // close input stream
    if (in != null) in.close();
  }

  /**
   * Reports the current progress.
   *
   * @return The progress as a percent value.
   * @throws java.io.IOException When something fails during the computation.
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() throws IOException {
    if(start == end) return 0.0F;
    else {
      float percent = Math.min(1.0F, (float)
        ((in != null ? in.getPos() : pos) - start) / (float) (end - start));
      if (LOG.isDebugEnabled()) LOG.debug("getProgress: percent -> " + percent);
      return percent;
    }
  }

  /**
   * Returns the current key.
   *
   * @return The current key or <code>null</code>.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
   */
  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  /**
   * The current value.
   *
   * @return The current value or <code>null</code>.
   * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
   */
  @Override
  public RedisCommand getCurrentValue() {
    return command;
  }

}
