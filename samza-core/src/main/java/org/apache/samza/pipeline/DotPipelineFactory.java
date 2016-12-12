/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.pipeline;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.api.PipelineFactory;
import org.apache.samza.pipeline.stream.PStream;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.*;

public class DotPipelineFactory implements PipelineFactory {
  private static final Logger log = LoggerFactory.getLogger(DotPipelineFactory.class);

  public static String SYSTEM = "system";
  public static String STREAM = "stream";
  public static String PARTITION = "partition";

  public static String SYSTEM_AUTO = "system-auto";
  public static String STREAM_AUTO = "stream-auto";
  public static int PARTITION_AUTO = -1;

  @Override
  public Pipeline create(Config config) {
    log.info("create pipeline from dot file");
    String dotFileName = config.get("job.pipeline.file", "");
    try {
      String content = new String(Files.readAllBytes(Paths.get(dotFileName)));
      return parseGraph(content);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new SamzaException(e);
    }
  }

  static Pipeline parseGraph(String content) throws Exception {
    String graph = content.substring(content.indexOf('{') + 1, content.lastIndexOf('}'));
    int exprStart = 0;
    List<NodesWithStream> list = new ArrayList<>();
    while (exprStart < graph.length()) {
      int exprEnd = graph.indexOf(';', exprStart);
      if (exprEnd < 0) exprEnd = graph.length();

      String expr = graph.substring(exprStart, exprEnd).trim();
      exprStart = exprEnd + 1;
      log.info("dot expr: {}", expr);

      NodesWithStream nodesWithStream = parseExpr(expr);
      if (nodesWithStream != null) {
        log.info(nodesWithStream.toString());
        list.add(nodesWithStream);
      }
    }

    // calculate the in/out degrees of each node to find sources and sinks
    Map<String, Integer> inDegrees = new HashMap<>();
    Map<String, Integer> outDegrees = new HashMap<>();
    list.forEach(nodesWithStream -> {
      List<String> nodes = nodesWithStream.nodes;
      for(int i = 0; i < nodes.size(); i++) {
        String node = nodes.get(i);
        int in = inDegrees.getOrDefault(node, 0);
        int out = outDegrees.getOrDefault(node, 0);
        if(i == 0) {
          ++out;
        } else if(i == nodes.size() - 1) {
          ++in;
        } else {
          ++in;
          ++out;
        }
        inDegrees.put(node, in);
        outDegrees.put(node, out);
      }
    });

    // find the sources, processors and sinks
    Set<String> sources = new HashSet<>();
    Set<String> sinks = new HashSet<>();
    Map<String, Processor> processors = new HashMap<>();
    inDegrees.keySet().forEach(node -> {
      if(inDegrees.get(node) == 0) {
        sources.add(node);
      } else if(outDegrees.get(node) == 0) {
        sinks.add(node);
      } else if(!processors.containsKey(node)){
        processors.put(node, new Processor(node));
      }
    });

    // build the pipeline
    PipelineBuilder builder = new PipelineBuilder();
    list.forEach(nodesWithStream -> {
      List<String> nodes = nodesWithStream.nodes;
      SystemStreamPartition ssp = nodesWithStream.systemStreamPartition;
      PStream pstream = new PStream("test", ssp, ssp.getPartition().getPartitionId());
      for(int i =0; i< nodes.size() - 1; i++) {
        String cur = nodes.get(i);
        String next = nodes.get(i+1);
        if(sources.contains(cur)) {
          builder.addInputStreams(processors.get(next), pstream);
        } else if(sinks.contains(next)) {
          builder.addOutputStreams(processors.get(cur), pstream);
        } else {
          builder.addIntermediateStreams(processors.get(cur), processors.get(next), pstream);
        }
      }
    });
    return builder.build();
  }

  static NodesWithStream parseExpr(String expr) throws Exception {
    if(!expr.contains("->")) return null;

    NodesWithStream procsWithStream = new NodesWithStream();
    String system = SYSTEM_AUTO;
    String stream = STREAM_AUTO;
    int partition = PARTITION_AUTO;

    // parse edge properties
    int propStart = expr.indexOf('[');
    if(propStart >= 0) {
      int propEnd = expr.indexOf(']');
      if(propEnd < 0) {
        throw new ParseException("no ] at the end of " + expr, expr.length() - 1);
      }
      String propExpr = expr.substring(propStart + 1, propEnd);
      String[] properties = propExpr.split(" ");
      for (String prop : properties) {
        String[] p = prop.split("=");
        if (p[0].equals(SYSTEM)) {
          system = p[1];
        } else if (p[0].equals(STREAM)) {
          stream = p[1];
        } else if (p[0].equals(PARTITION)) {
          partition = Integer.valueOf(p[1]);
        }
      }
    }
    procsWithStream.systemStreamPartition = new SystemStreamPartition(system, stream, new Partition(partition));
    procsWithStream.nodes = new ArrayList<>();

    // parse the nodes
    String nodeExpr = (propStart > 0) ? expr.substring(0, propStart) : expr;
    String[] nodes = nodeExpr.split("->");
    for(String node : nodes) {
      String n = node.trim();
      procsWithStream.nodes.add(n);
    }

    //TODO: validate that fromSource/intoSink has only one processor
    return procsWithStream;
  }

  private static final class NodesWithStream {
    List<String> nodes;
    SystemStreamPartition systemStreamPartition;

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      nodes.forEach(n -> builder.append(n).append(" "));
      builder.append(" connected with ").append(systemStreamPartition.toString());
      return builder.toString();
    }
  }
}
