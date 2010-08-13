/* File:    AppendOnyFileInputFormat.java
 * Created: July 21, 2010
 * Author:  Lars George
 *
 * Copyright (c) 2009 larsgeorge.com
 */

package com.collective.hadoop.mapred;

// Java libs
import java.io.IOException;
// Apache libs
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// Collective libs
import com.collective.io.redis.RedisCommand;

/**
 * Special input format that handles the Redis AOF files.
 *
 * @author Lars George
 */
public class AppendOnlyFileInputFormat extends FileInputFormat<LongWritable, RedisCommand> {

  /**
   * Creates a special record reader.
   *
   * @param split  The handed in split details defining the reader's range.
   * @param context  The current task context.
   * @return The newly created record reader.
   * @throws java.io.IOException When creating the reader fails.
   */
  public RecordReader<LongWritable, RedisCommand> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    context.setStatus(split.toString());
    return new AppendOnlyFileRecordReader();
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
