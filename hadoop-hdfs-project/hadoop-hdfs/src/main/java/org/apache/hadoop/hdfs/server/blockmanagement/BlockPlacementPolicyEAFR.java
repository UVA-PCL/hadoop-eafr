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
        return fallbackPolicy.chooseTarget(
            srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes, excludedNodes,
            blocksize, storagePolicy, flags);
    }
    return super.chooseTarget(srcPath, numOfReplicas, writer, chosenNodes, returnChosenNodes,
        excludedNodes, blocksize, storagePolicy, flags);
  }


  protected DatanodeDescriptor chooseTargetWithHighestStorage(
      int numOfReplicas,
      final Set<Node> excludedNodes,
      final long blocksize,
      final int maxNodesPerRack,
      final List<DatanodeStorageInfo> results,
      final boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) {
    long sum = 0;
    TreeMap<Long, DatanodeStorageInfo> capacity = new TreeMap<Long, DatanodeStorageInfo>();
    NavigableMap<Double, DatanodeStorageInfo> preferCapacity =
        new TreeMap<Double, DatanodeStorageInfo>();
    ArrayList<Long> durations = new ArrayList<Long>();
    TreeMap<Long, DatanodeStorageInfo> ewma = new TreeMap<Long, DatanodeStorageInfo>();
    NavigableMap<Double, DatanodeStorageInfo> preferDelay =
        new TreeMap<Double, DatanodeStorageInfo>();
    HashMap<DatanodeStorageInfo, Double> revPreferCapacity =
        new HashMap<DatanodeStorageInfo, Double>();
    HashMap<DatanodeStorageInfo, Double> revPreferDelay =
        new HashMap<DatanodeStorageInfo, Double>();
    NavigableMap<Double, DatanodeStorageInfo> weightedProbability =
        new TreeMap<Double, DatanodeStorageInfo>();
    double alpha = .8;

    for (DatanodeStorageInfo storage : results) {
      capacity.put(storage.getCapacity(), storage);
    }

    for (DatanodeStorageInfo storage : results) {
      ewma.put(storage.getDatanodeDescriptor().getBlockTransferTime(), storage);
    }
    for (int i = 0; i < results.size(); i++) {
      sum += 1.0 / i;
    }
    for (Long key : capacity.keySet()) {
      int j = 1;
      double probability = (1 / j) / (sum);
      preferCapacity.put(probability, capacity.get(key));
      preferDelay.put(probability, capacity.get(key));
    }
    for (Double key : preferCapacity.keySet()) {
      revPreferCapacity.put(preferCapacity.get(key), key);
    }
    for (Double key : preferDelay.keySet()) {
      revPreferDelay.put(preferDelay.get(key), key);
    }

    for (DatanodeStorageInfo key : revPreferCapacity.keySet()) {
      if (revPreferDelay.containsKey(key)) {
        double weightedProb = revPreferCapacity.get(key) + revPreferDelay.get(key);
        weightedProbability.put(weightedProb, key);
      }
    }
    Random random = new Random();
    double totalProbability = 0;
    for (Double key : weightedProbability.keySet()) {
      totalProbability += key;
    }
    double value = random.nextDouble() * totalProbability;
    DatanodeStorageInfo dns = weightedProbability.higherEntry(value).getValue();
    DatanodeDescriptor chosenNode = dns.getDatanodeDescriptor();

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
    while (numOfReplicas > 0) {
      DatanodeDescriptor chosenNode =
          chooseTargetWithHighestStorage(
              numOfReplicas,
              excludedNodes,
              blocksize,
              maxNodesPerRack,
              results,
              avoidStaleNodes,
              storageTypes);
      if (chosenNode == null) {
        break;
      }
      Preconditions.checkState(
          excludedNodes.add(chosenNode),
          "chosenNode " + chosenNode + " is already in excludedNodes " + excludedNodes);
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

  /**
   * Randomly choose <i>numOfReplicas</i> targets from the given <i>scope</i>.
   * @return the first chosen node, if there is any.
   */
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
    return chooseTargetForEAFR(numOfReplicas, excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
  }
}

