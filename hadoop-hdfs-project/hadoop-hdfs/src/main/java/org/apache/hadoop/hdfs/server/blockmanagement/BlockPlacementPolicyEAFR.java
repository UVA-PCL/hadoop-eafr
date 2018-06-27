/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.hdfs.server.datanode.BlockReceiver;
import org.apache.hadoop.hdfs.server.namenode.AccessLog;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class BlockPlacementPolicyEAFR extends BlockPlacementPolicyDefault {

  private static final String enableDebugLogging =
      "For more information, please enable DEBUG log level on "
          + BlockPlacementPolicy.class.getName() + " and "
          + NetworkTopology.class.getName();

  private static final ThreadLocal<StringBuilder> debugLoggingBuilder
      = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  private BlockPlacementPolicy fallbackPolicy = new BlockPlacementPolicyDefault();

  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap,
                         Host2NodesMap host2datanodeMap) {
      super.initialize(conf, stats, clusterMap, host2datanodeMap);
      fallbackPolicy.initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenNodes,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    final BlockStoragePolicy storagePolicy,
                                    EnumSet<AddBlockFlag> flags) {
    boolean isHotFile=AccessLog.isHotFile(srcPath);
    boolean isColdFile=AccessLog.isColdFile(srcPath);
    if (!isHotFile) {
      LOG.warn("chooseTarget --- not a hot file, using fallback");
      return fallbackPolicy.chooseTarget(
          srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
          blocksize, storagePolicy, flags);
    }
    return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes,
        excludedNodes, blocksize, storagePolicy, flags);
  }

  private long getRemainingForTypes(DatanodeDescriptor dn, Set<StorageType> storageTypes) {
    long currentRemaining = 0L;
    for (DatanodeStorageInfo storage : dn.getStorageInfos()) {
      if (storageTypes.contains(storage.getStorageType())) {
        currentRemaining += storage.getRemaining();
      }
    }
    return currentRemaining;
  }


  protected DatanodeDescriptor chooseTargetWithEAFRPolicy(
      final List<DatanodeDescriptor> allNodes,
      final EnumMap<StorageType, Integer> storageTypes) {
    NavigableMap<Double, DatanodeDescriptor> preferCapacity =
        new TreeMap<Double, DatanodeDescriptor>();
    TreeMap<Long, DatanodeDescriptor> ewma = new TreeMap<Long, DatanodeDescriptor>();
    NavigableMap<Double, DatanodeDescriptor> preferDelay =
        new TreeMap<Double, DatanodeDescriptor>();
    HashMap<DatanodeDescriptor, Double> revPreferCapacity =
        new HashMap<DatanodeDescriptor, Double>();
    HashMap<DatanodeDescriptor, Double> revPreferDelay =
        new HashMap<DatanodeDescriptor, Double>();
    NavigableMap<Double, DatanodeDescriptor> weightedProbability =
        new TreeMap<Double, DatanodeDescriptor>();
    double alpha = .8;

    if (allNodes.size() == 0) {
      return null;
    }

    LOG.debug("candidate nodes are " + allNodes);

    List<DatanodeDescriptor> nodesByCapacity = new ArrayList<DatanodeDescriptor>(allNodes);
    nodesByCapacity.sort(new Comparator<DatanodeDescriptor>() {
      @Override
      public int compare(DatanodeDescriptor a, DatanodeDescriptor b) {
        if (a == b) return 0;
        long aRemaining = getRemainingForTypes(a, storageTypes.keySet());
        long bRemaining = getRemainingForTypes(b, storageTypes.keySet());
        if (aRemaining < bRemaining) {
          return 1;
        } else if (aRemaining > bRemaining) {
          return -1;
        } else {
          return 0;
        }
      }
    });
    LOG.debug("after sorting, nodes by capacity are " + nodesByCapacity);
    List<DatanodeDescriptor> nodesByDelay = new ArrayList<DatanodeDescriptor>(allNodes);
    nodesByDelay.sort(new Comparator<DatanodeDescriptor>() {
      @Override
      public int compare(DatanodeDescriptor a, DatanodeDescriptor b) {
        if (a == b) return 0;
        double aTime = a.getBlockTransferTime();
        double bTime = b.getBlockTransferTime();
        if (aTime > bTime) {
          return 1;
        } else if (aTime < bTime) {
          return -1;
        } else {
          return 0;
        }
      }
    });
    LOG.debug("after sorting, nodes by delay are " + nodesByDelay);
    {
      double sum = 1.0;
      int index = 1;
      for (DatanodeDescriptor dn : nodesByCapacity) {
        double probability = 1.0 / sum;
        preferCapacity.put(probability, dn);
        sum += 1.0 / ++index;
      }
    }
    {
      double sum = 1.0;
      int index = 0;
      for (DatanodeDescriptor dn : nodesByDelay) {
        double probability = 1.0 / sum;
        preferDelay.put(probability, dn);
        sum += 1.0 / ++index; 
      }
    }
    for (Double key : preferCapacity.keySet()) {
      revPreferCapacity.put(preferCapacity.get(key), key);
    }
    for (Double key : preferDelay.keySet()) {
      revPreferDelay.put(preferDelay.get(key), key);
    }

    double totalProbability = 0;
    for (DatanodeDescriptor key : revPreferCapacity.keySet()) {
      if (revPreferDelay.containsKey(key)) {
        /* FIXME: should use beta to determine weighting */
        double currentProb = revPreferCapacity.get(key) + revPreferDelay.get(key);
        LOG.warn("probability for " + key.getNetworkLocation() + " -> " + currentProb);
        totalProbability += currentProb;
        weightedProbability.put(totalProbability, key);
      }
    }
    Random random = new Random();
    double value = random.nextDouble() * totalProbability;
    LOG.debug("Choose value " + value + " based on total " + totalProbability);
    DatanodeDescriptor chosenNode = weightedProbability.higherEntry(value).getValue();

    return chosenNode;
  }

  protected long EWMA(long value, double alpha, long oldvalue) {
    if (oldvalue == (Long) null) {
      oldvalue = value;
      return value;
    }
    long newvalue = (long) (alpha * value + (1 - alpha) * (oldvalue));
    return newvalue;
  }

  protected DatanodeStorageInfo chooseTargetForEAFR(
      int numOfReplicas,
      String scope,
      Set<Node> excludedNodes,
      long blocksize,
      int maxNodesPerRack,
      List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    StringBuilder builder = null;
    if (LOG.isDebugEnabled()) {
      builder = debugLoggingBuilder.get();
      builder.setLength(0);
      builder.append("[");
    }
    boolean badTarget = false;
    DatanodeStorageInfo firstChosen = null;
    List<Node> allNodes = clusterMap.getLeaves(scope);
    allNodes.removeAll(excludedNodes);
    List<DatanodeDescriptor> allNodesAsDNs = new ArrayList<DatanodeDescriptor>();
    for (Node n : allNodes) {
      if (n != null) {
        allNodesAsDNs.add((DatanodeDescriptor) n);
      }
    }
    while (numOfReplicas > 0) {
      LOG.debug("candidates are " + allNodes + " and excluded is " + excludedNodes);
      DatanodeDescriptor chosenNode = chooseTargetWithEAFRPolicy(allNodesAsDNs, storageTypes);
      if (chosenNode == null) {
        break;
      }
      Preconditions.checkState(
          excludedNodes.add(chosenNode),
          "chosenNode " + chosenNode + " is already in excludedNodes " + excludedNodes);
      allNodes.remove(chosenNode);
      if (LOG.isDebugEnabled()) {
        builder.append("\nNode ").append(NodeBase.getPath(chosenNode)).append(" [");
      }
      DatanodeStorageInfo storage = null;
      if (isGoodDatanode(chosenNode, maxNodesPerRack, considerLoad, results, avoidStaleNodes)) {
        for (Iterator<Map.Entry<StorageType, Integer>> iter = storageTypes.entrySet().iterator();
            iter.hasNext(); ) {
          Map.Entry<StorageType, Integer> entry = iter.next();
          storage = chooseStorage4Block(chosenNode, blocksize, results, entry.getKey());
          if (storage != null) {
            numOfReplicas--;
            if (firstChosen == null) {
              firstChosen = storage;
            }
            // add node (subclasses may also add related nodes) to excludedNode
            addToExcludedNodes(chosenNode, excludedNodes);
            int num = entry.getValue();
            if (num == 1) {
              iter.remove();
            } else {
              entry.setValue(num - 1);
            }
            break;
          }
        }

        if (LOG.isDebugEnabled()) {
          builder.append("\n]");
        }

        // If no candidate storage was found on this DN then set badTarget.
        badTarget = (storage == null);
      }
    }
    if (numOfReplicas > 0) {
      String detail = enableDebugLogging;
      if (LOG.isDebugEnabled()) {
        if (badTarget && builder != null) {
          detail = builder.toString();
          builder.setLength(0);
        } else {
          detail = "";
        }
      }
      throw new NotEnoughReplicasException(detail);
    }

    return firstChosen;
  }

  @Override
  protected DatanodeStorageInfo chooseRandom(int numOfReplicas,
                            String scope,
                            Set<Node> excludedNodes,
                            long blocksize,
                            int maxNodesPerRack,
                            List<DatanodeStorageInfo> results,
                            boolean avoidStaleNodes,
                            EnumMap<StorageType, Integer> storageTypes)
                            throws NotEnoughReplicasException {
    return chooseTargetForEAFR(numOfReplicas, scope, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
  }
}

