/**
 * Copyright (C) 2009-2013 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.roadrunner;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.CommandLine;

/**
 * A container class for the user-provided options through the command line.
 *
 * It also sets sensible defaults when no or a subset of arguments are
 * provided.
 */
public class GlobalConfig {

  private List<String> nodes;
  private String bucket;
  private String password;
  private int numThreads;
  private int numClients;
  private int numDocs;
  private int readratio;
  private int writeratio;
  private int samplingCount;
  private int batchSize;
  private int ramp;
  private String className;
  private String phase;
  private int minThinkTime;
  private int maxThinkTime;

  private String keyPrefix = "RoadRunnerDoc";

  /**
   * Create the GlobalConfig object by parsing the command line args and
   * applying defaults.
   *
   * @param args The passed in command line arguments
   * @return global config object
   */
  public GlobalConfig(final CommandLine args) {
    this.nodes = prepareNodeList(args.hasOption(RoadRunner.OPT_NODES)
      ? args.getOptionValue(RoadRunner.OPT_NODES) : RoadRunner.DEFAULT_NODES);

    this.bucket = args.hasOption(RoadRunner.OPT_BUCKET)
      ? args.getOptionValue(RoadRunner.OPT_BUCKET) : RoadRunner.DEFAULT_BUCKET;

    this.password = args.hasOption(RoadRunner.OPT_PASSWORD)
      ? args.getOptionValue(RoadRunner.OPT_PASSWORD) : RoadRunner.DEFAULT_PASSWORD;

    this.numThreads = Integer.parseInt(args.hasOption(RoadRunner.OPT_NUM_THREADS)
      ? args.getOptionValue(RoadRunner.OPT_NUM_THREADS) : RoadRunner.DEFAULT_NUM_THREADS);

    this.numClients = Integer.parseInt(args.hasOption(RoadRunner.OPT_NUM_CLIENTS)
      ? args.getOptionValue(RoadRunner.OPT_NUM_CLIENTS) : RoadRunner.DEFAULT_NUM_CLIENTS);

    this.numDocs = Integer.parseInt(args.hasOption(RoadRunner.OPT_NUM_DOCS)
      ? args.getOptionValue(RoadRunner.OPT_NUM_DOCS) : RoadRunner.DEFAULT_NUM_DOCS);

    this.readratio = Integer.parseInt(args.hasOption(RoadRunner.OPT_READRATIO)
      ? args.getOptionValue(RoadRunner.OPT_READRATIO) : RoadRunner.DEFAULT_READ_RATIO);

    this.writeratio = Integer.parseInt(args.hasOption(RoadRunner.OPT_WRITERATIO)
            ? args.getOptionValue(RoadRunner.OPT_WRITERATIO) : RoadRunner.DEFAULT_WRITE_RATIO);

    int sampleRate = Integer.parseInt(args.hasOption(RoadRunner.OPT_SAMPLING)
      ? args.getOptionValue(RoadRunner.OPT_SAMPLING) : RoadRunner.DEFAULT_SAMPLING);
    this.samplingCount = this.numDocs / sampleRate;

    this.phase = args.hasOption(RoadRunner.OPT_PHASE)
      ? args.getOptionValue(RoadRunner.OPT_PHASE) : RoadRunner.DEFAULT_PHASE;

    this.batchSize = Integer.parseInt(args.hasOption(RoadRunner.OPT_BATCHSIZE)
            ? args.getOptionValue(RoadRunner.OPT_BATCHSIZE) : RoadRunner.DEFAULT_BATCHSIZE);

    this.ramp = Integer.parseInt(args.hasOption(RoadRunner.OPT_RAMP)
      ? args.getOptionValue(RoadRunner.OPT_RAMP) : RoadRunner.DEFAULT_RAMP);

    this.className = args.hasOption(RoadRunner.OPT_CLASS_NAME)
      ? args.getOptionValue(RoadRunner.OPT_CLASS_NAME) : RoadRunner.DEFAULT_CLASS;

    this.minThinkTime = Integer.parseInt(args.hasOption(RoadRunner.OPT_MINTHINKTIME)
            ? args.getOptionValue(RoadRunner.OPT_MINTHINKTIME) : RoadRunner.DEFAULT_MIN_THINKTIME);

    this.maxThinkTime = Integer.parseInt(args.hasOption(RoadRunner.OPT_MAXTHINKTIME)
            ? args.getOptionValue(RoadRunner.OPT_MAXTHINKTIME) : RoadRunner.DEFAULT_MAX_THINKTIME);
  }

  /**
   * Converts the node string into a list of URIs.
   *
   * @param nodes The node list as a single string.
   * @return The nodes converted to a list of Strings (one for each node).
   */
  private static List<String> prepareNodeList(final String nodes) {
    return Arrays.asList(nodes.split(","));
  }

  /**
   * @return the nodes
   */
  public List<String> getNodes() {
    return nodes;
  }

  public String getBucket() {
    return bucket;
  }

  public String getPassword() {
    return password;
  }

  public int getNumThreads() {
    return numThreads;
  }

  public int getNumClients() {
    return numClients;
  }

  public long getNumDocs() {
    return numDocs;
  }

  public int getSamplingCount() {
    return samplingCount;
  }

  public int getRamp() {
    return ramp;
  }

  public String getClassName() {
    return className;
  }

  public int getMinThinkTime() { return minThinkTime; }

  public int getMaxThinkTime() { return maxThinkTime; }

  public int getReadratio() { return readratio; }

  public int getWriteratio() { return writeratio; }

  public int getBatchSize() { return batchSize; }

  public String getPhase() { return phase; }

  public String getKeyPrefix() { return keyPrefix; }

  @Override
  public String toString() {
    return "GlobalConfig{" + "nodes=" + nodes + ", bucket=" + bucket
      + ", password=" + password + ", numThreads=" + numThreads
      + ", numClients=" + numClients + ", numDocs=" + numDocs
      + ", sampling=" + samplingCount + ", ramp=" + ramp + "}";
  }
}
