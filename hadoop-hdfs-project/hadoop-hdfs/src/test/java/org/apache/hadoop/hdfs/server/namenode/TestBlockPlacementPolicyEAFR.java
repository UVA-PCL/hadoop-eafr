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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockPlacementPolicyEAFR;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.AccessLog;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.net.StaticMapping;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBlockPlacementPolicyEAFR {

  private static final int DEFAULT_BLOCK_SIZE = 1024;
  private MiniDFSCluster cluster = null;
  private NamenodeProtocols nameNodeRpc = null;
  private FSNamesystem namesystem = null;
  private PermissionStatus perm = null;

  private static final int NODES = 5;

  @Before
  public void setup() throws IOException {
    StaticMapping.resetMap();
    Configuration conf = new HdfsConfiguration();
    final ArrayList<String> hostList = new ArrayList<String>();
    final ArrayList<String> rackList = new ArrayList<String>();
    final long[][] capacityList = new long[NODES][1];
    final StorageType[][] storageTypeList = new StorageType[NODES][1];
    for (int i = 0; i < NODES; i++) {
      hostList.add("/host" + i);
      rackList.add("/rack" + i);
      capacityList[i][0] = (i + 1) * 1024L * 1024L * 1024L;
      storageTypeList[i][0] = StorageType.DISK;
    }
    conf.setClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
        BlockPlacementPolicyEAFR.class,
        BlockPlacementPolicy.class);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DEFAULT_BLOCK_SIZE);
    conf.setInt(DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_KEY, DEFAULT_BLOCK_SIZE / 2);
    conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "127.0.0.1");
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(hostList.size())
        .racks(rackList.toArray(new String[rackList.size()]))
        .hosts(hostList.toArray(new String[hostList.size()]))
        .storagesPerDatanode(1)
        .storageTypes(storageTypeList)
        .storageCapacities(capacityList)
        .build();
    cluster.waitActive();
    int nodeNum = 0;
    for (DataNode dn : cluster.getDataNodes()) {
      dn.overrideBlockTransferTime(1000L - nodeNum++);
      String name = dn.getConf().get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
      System.err.println("datanode with xfer addr " + dn.getXferAddress() + " has fake hostname " + name);
      System.err.println("datanode with ipc port " + dn.getIpcPort() + " has fake hostname " + name);
    }
    cluster.triggerHeartbeats();
    nameNodeRpc = cluster.getNameNodeRpc();
    namesystem = cluster.getNamesystem();
    perm = new PermissionStatus("TestBlockPlacementPolicyEAFR", null,
        FsPermission.getDefault());
    for (int i = 0; i < 10000; ++i) {
      AccessLog.hotFiles.add("/hotfile" + i);
      AccessLog.coldFiles.add("/coldfile" + i);
    }
    System.err.println("IN TEST");
  }

  @After
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testEmpty() {
  }

  @Test
  public void testPreferMoreRemainingLessDelay() throws IOException {
    String clientMachine = "client.foo.com";
    HashMap<String, Integer> counts = new HashMap<String, Integer>();
    for (int i = 0; i < 10000; ++i) {
      short replication = 1;
      String src = "/hotfile" + i;
      // Create the file with client machine
      HdfsFileStatus fileStatus = namesystem.startFile(src, perm,
          clientMachine, clientMachine, EnumSet.of(CreateFlag.CREATE), true,
          replication, DEFAULT_BLOCK_SIZE, null, false);

      //test chooseTarget for new file
      LocatedBlock locatedBlock = nameNodeRpc.addBlock(src, clientMachine,
          null, null, fileStatus.getFileId(), null, null);
      for (DatanodeInfo node : locatedBlock.getLocations()) {
        int port = node.getIpcPort();
        DataNode dn = cluster.getDataNode(port);
        String name = dn.getConf().get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        counts.put(name, counts.getOrDefault(name, 0) + 1);
      }
    }
    System.err.println("counts are " + counts);
    String[] allNames = counts.keySet().toArray(new String[0]);
    Arrays.sort(allNames);
    long previousCount = -1;
    /* lower named nodes have lower capacity -- should be less preferred */
    for (String key : allNames) {
      assertTrue("with high probability, node with lower capacity+higher delay should be chosen less: " + 
                 "previousCount = " + previousCount + " versus count " + counts.get(key),
                 previousCount < counts.get(key));
      previousCount = counts.get(key);
    }
    assertTrue("with high probability, all nodes should be used at least once", allNames.length == NODES);
  }
}
