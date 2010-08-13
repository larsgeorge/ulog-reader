/* File:    UlogRecordReader.java
 * Created: May 22, 2009
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.collective.hadoop.mapred;

// Java libs
import java.io.IOException;
import java.io.PushbackInputStream;
// Apache Commons libs
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
// Hadoop/HBase libs
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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
import com.collective.io.tyrant.UlogReader;

/**
 * Reads the Tokyo Tyrant update log binary files.
 *  
 * @author Lars George
 */
public class UlogRecordReader extends RecordReader<LongWritable, Command> {

  static final Log LOG = LogFactory.getLog(UlogRecordReader.class);

  /** Can be used to set the buffer during ulog reads. */
  public static final String BUFFER_SIZE         = "UlogRecordReader.bufferSize";
  /** Semaphore used to flag already processed files. */
  public static final String SEMAPHORE           = "UlogRecordReader.semaphore";
  /** Do a dry run, do not set semaphores. */
  public static final String DRYRUN              = "UlogRecordReader.dryRun";
  
  private Configuration configuration = null;
  private long start;
  private long pos;
  private long end;
  private boolean stillInChunk = true;
  private FSDataInputStream in = null;
  private PushbackInputStream pin = null;
  private UlogReader reader = null;
  private byte[] headers = new byte[UlogReader.SIZE_HEADER];
  private CompressionCodecFactory compressionCodecs = null;
  private LongWritable key = null;
  private Command value = null;
  private Path inputFile = null;
  private long startTime = System.currentTimeMillis();
  private long countCalls = 0;
  private long totalTime = 0;
  private long lastCall = -1;
  private String semaphore = null;
  private boolean dryrun = false;
  
  /**
   * Initializes the reader.
   * 
   * @param genericSplit  The current split.
   * @param context  The job context.
   * @throws IOException When the split is faulty.
   * @throws InterruptedException When the job is interrupted.
   * @see org.apache.hadoop.mapreduce.RecordReader#initialize(
   *   org.apache.hadoop.mapreduce.InputSplit, 
   *   org.apache.hadoop.mapreduce.TaskAttemptContext)
   */
  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext context) 
  throws IOException, InterruptedException {
    LOG.info("initialize: start time -> " + startTime);
    LOG.info("initialize: start init -> " + (System.currentTimeMillis() - startTime));
    configuration = context.getConfiguration();
    dryrun = configuration.getBoolean(DRYRUN, false);
    semaphore = configuration.get(SEMAPHORE);
    LOG.info("initialize: semaphore -> " + semaphore);
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
    FileSystem fs = inputFile.getFileSystem(configuration);
    in = fs.open(inputFile);
    // skip to the start offset 
    if (start != 0L) in.seek(start);
    if (LOG.isDebugEnabled()) LOG.debug("initialize: start -> " + start + ", end -> " + end);
    if (LOG.isDebugEnabled()) LOG.debug("initialize: skip to pos -> " + pos);
    if (LOG.isDebugEnabled()) LOG.debug("initialize: stream pos -> " + in.getPos());
    pin = new PushbackInputStream(codec != null ? 
      codec.createInputStream(in) : in, UlogReader.SIZE_HEADER); // headers
    // check if we should use a specific buffer size
    String buffSize = configuration.get(BUFFER_SIZE);
    reader = buffSize == null ? new UlogReader(pin) : 
      new UlogReader(in, Integer.parseInt(buffSize));
    // skip leading data from the boundary-unaware file split
    stillInChunk = findFirstObject();
    if (LOG.isDebugEnabled()) LOG.debug("initialize: found object at pos -> " + pos);
    if (LOG.isDebugEnabled()) LOG.debug("initialize: new stream pos -> " + in.getPos());
    LOG.info("initialize: done init -> " + (System.currentTimeMillis() - startTime));
  }
  
  /**
   * Resturns the current processed file name. For debugging mainly.
   * 
   * @return Returns the input file name.
   */
  public Path getInputFile() {
    return inputFile;
  }

  /**
   * Finds the start of a block from the stream.
   * 
   * @return <code>true</code> when an object was found.
   * @throws IOException When reading from the stream fails.
   */
  private boolean findFirstObject() throws IOException {
    while (true) {
      int b = pin.read();
      if (b == -1) return false;
      pos++;
      if (pos % 100000 == 0) LOG.info("findFirstObject: scanning pos -> " + pos);
      // check if we possibly have a magic header signature
      if (b == UlogReader.TCULMAGICNUM) {
        // put it back and read a larger chunk
        pin.unread(UlogReader.TCULMAGICNUM);
        pin.read(headers);
        // put that chunk back too as we just need to check here
        pin.unread(headers);
        // now check if this is a true command object start
        if (UlogReader.isValidHeader(headers)) return in.getPos() < end;
        else pin.read(); // otherwise skip this false positive magic number
      }
    }
  }

  /**
   * Changes reading from a single line to multiple ones. Handles the problem
   * of reading beyond the chunk. 
   * 
   * @return <code>true</code> when data was read.
   * @throws IOException When reading the data fails.
   * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
   */
  public boolean nextKeyValue() throws IOException {
    if (!stillInChunk) {
      LOG.info("nextKeyValue: reached end of chunk -> " + (System.currentTimeMillis() - startTime));
      return false;
    }
    if (key == null) key = new LongWritable();
    key.set(in.getPos());
    value = reader.next();
    if (in.getPos() >= end) stillInChunk = false;
    countCalls++;
    long now = System.currentTimeMillis(); 
    if (lastCall > -1) totalTime += (now - lastCall); 
    lastCall = now;
    return true;  
  }

  /**
   * Closes the input.
   * 
   * @throws IOException When closing the input fails.
   * @see org.apache.hadoop.mapreduce.RecordReader#close()
   */
  @Override
  public void close() throws IOException {
	// close input stream
    if (in != null) in.close();    
    // check if semaphore is to be set
    if (semaphore != null && inputFile != null) {
      FileSystem fs = inputFile.getFileSystem(configuration);
      if (fs.isFile(inputFile)) {
        Path sem1 = new Path(inputFile.toString() + semaphore);
        Path sem2 = new Path(inputFile.toString() + "-" + start + "-" + end + semaphore);
        if (!dryrun) {
          if (fs.createNewFile(sem1))
            LOG.info("close: setting semaphore -> " + sem1);
          if (fs.createNewFile(sem2))
            LOG.info("close: setting semaphore -> " + sem2);
        } else LOG.info("close: DRYRUN - Not setting semaphore -> " + sem1 + " and " + sem2);
      }
    }
    // print out timing details
    long elapsed = System.currentTimeMillis() - startTime;
    LOG.info("close: elapsed -> " + elapsed);
    LOG.info("close: avg time per call -> " + (elapsed / countCalls));
    LOG.info("close: avg between calls -> " + (totalTime / countCalls));
  }

  /**
   * Reports the current progress.
   * 
   * @return The progress as a percent value.
   * @throws IOException When something fails during the computation.
   * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
   */
  @Override
  public float getProgress() throws IOException {
    if (LOG.isDebugEnabled()) LOG.debug("getProgress: pos -> " + pos + ", start -> " + start + ", end -> " + end + ", in.pos -> " + (in != null ? in.getPos() : -1));
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
  public Command getCurrentValue() {
    return value;
  }

} 
