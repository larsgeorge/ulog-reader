/* File:    UlogInputFormat.java
 * Created: Mar 22, 2009
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.collective.hadoop.mapred;

import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Special input format that handles the Tokyo Tyrant update log files.
 *  
 * @author Lars George
 */
public class UlogInputFormat extends FileInputFormat<LongWritable, Command> {

  /**
   * Creates a special record reader.
   *  
   * @param split  The handed in split details defining the reader's range.
   * @param job  The current job details.
   * @param reporter  The progress reporter instance.
   * @return The newly created record reader.
   * @throws IOException When creating the reader fails.
   * @see org.apache.hadoop.mapred.FileInputFormat#getRecordReader(org.apache.hadoop.mapred.InputSplit, org.apache.hadoop.mapred.JobConf, org.apache.hadoop.mapred.Reporter)
   */
  public RecordReader<LongWritable, Command> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    context.setStatus(split.toString());
    return new UlogRecordReader();
  }

  /**
   * Checks if a file is splitable.
   *  
   * @param context  The current job context.
   * @param file  The current file.
   * @return <code>true</code> if the current file is splitable.
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    CompressionCodec codec = new CompressionCodecFactory(
      context.getConfiguration()).getCodec(file);
    return codec == null;
  }
  
} 
