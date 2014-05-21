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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.datatransfer.Sender;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.net.NetUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;


/**
 * Test to ensure that new replicas for existing blocks are created on the
 * same StorageType as the remaining blocks. This is useful e.g. for supporting
 * ARCHIVAL storage by ensuring that loss of an ARCHIVAL DataNode does not
 * result in replication to DISK.
 */
public class TestReplicationWithinStorageType {
  static final Log LOG = LogFactory.getLog(TestReplicationWithinStorageType.class);

  protected MiniDFSCluster cluster;
  private DistributedFileSystem fs;
  private Configuration conf;
  private DFSClient client;
  private final static short INITIAL_REPLICAS = 2;
  private final static int NUM_BLOCKS_PER_FILE = 50;
  private final static int BLOCK_LENGTH = 1024;

  @Before
  public void startUpCluster() throws IOException {
    assertTrue(INITIAL_REPLICAS > 1);
    conf = new HdfsConfiguration();
    conf.setLong(DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 500);

    // Speed up replication for underReplicated blocks.
    conf.setInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION, 100);
    conf.setLong(
        DFSConfigKeys.DFS_BLOCKREPORT_INTERVAL_MSEC_KEY, 15 * 1000);

    // Enable quicker dead DN detection.
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY, 15 * 1000);
    cluster = new MiniDFSCluster.Builder(conf)
        .numDataNodes(INITIAL_REPLICAS)
        .build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
    client = fs.getClient();

    for (DataNode dn : cluster.getDataNodes()) {
      LOG.info("Initial DataNode " + dn.getDatanodeUuid());
    }
  }

  @After
  public void shutDownCluster() throws IOException {
    fs.close();
    cluster.shutdownDataNodes();
    cluster.shutdown();
    cluster = null;
    fs = null;
    client = null;
  }

  private void checkReplicaCountOnStorageType(Path file,
                                              final StorageType expectedStorageType,
                                              final int numExpectedReplicas)
      throws IOException {
    LocatedBlocks locations = client.getLocatedBlocks(file.toString(), 0);
    for (LocatedBlock location : locations.getLocatedBlocks()) {
      int numActualReplicas = 0;
      for (int i = 0; i < location.getLocations().length; ++i) {
        if (location.getStorageTypes()[i] == expectedStorageType) {
          ++numActualReplicas;
        }
      }
      assertThat(numActualReplicas, is(numExpectedReplicas));
    }
    
  }

  @Test(timeout=600000)
  public void testDefaultStorageIsChosen() throws IOException {
    // Start more DataNodes with ARCHIVAL storage.
    LOG.info("Starting additional ARCHIVAL Datanodes");
    cluster.startDataNodesWithStorageType(conf, INITIAL_REPLICAS, StorageType.ARCHIVAL);

    // Create a file with 50 blocks.
    Path filePath = new Path("/file1");
    LOG.info("Creating file");
    DFSTestUtil.createFile(fs, filePath, BLOCK_LENGTH,
        BLOCK_LENGTH * NUM_BLOCKS_PER_FILE,
        BLOCK_LENGTH, INITIAL_REPLICAS, -1);

    // Ensure that none of the file replicas are on archival storage.
    checkReplicaCountOnStorageType(filePath, StorageType.DEFAULT, INITIAL_REPLICAS);
  }

  @Test(timeout=600000)
  public void testExistingStorageTypeIsRespected()
      throws IOException, InterruptedException, TimeoutException {
    // Create a file with multiple blocks.
    Path filePath = new Path("/file1");
    DFSTestUtil.createFile(fs, filePath, BLOCK_LENGTH,
                           BLOCK_LENGTH * NUM_BLOCKS_PER_FILE,
                           BLOCK_LENGTH, INITIAL_REPLICAS, -1);

    // Start two more DataNodes with ARCHIVAL storage.
    LOG.info("Starting additional ARCHIVAL Datanodes");
    cluster.startDataNodesWithStorageType(conf, INITIAL_REPLICAS, StorageType.ARCHIVAL);
    cluster.waitActive();

    moveReplicasToStorageType(filePath, StorageType.ARCHIVAL, Short.MAX_VALUE);
    DFSTestUtil.waitReplicationForever(fs, filePath, INITIAL_REPLICAS);
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, INITIAL_REPLICAS);

    // Shutdown one ARCHIVAL DN and wait for replicas to be detected as gone.
    shutdownOneDataNodeWithStorageType(StorageType.ARCHIVAL);
    DFSTestUtil.waitReplicationForever(fs, filePath, (short) (INITIAL_REPLICAS - 1));

    // Start another ARCHIVAL DN and wait for new replicas to be created.
    cluster.startDataNodesWithStorageType(conf, 1, StorageType.ARCHIVAL);

    // Ensure that a new replica for each block was created on ARCHIVAL storage and
    // not on DISK.
    DFSTestUtil.waitReplicationForever(fs, filePath, INITIAL_REPLICAS);
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, INITIAL_REPLICAS);
  }

  @Test(timeout=600000)
  public void testExistingStorageTypeIsRespected2()
      throws IOException, InterruptedException, TimeoutException {
    // Create a file with multiple blocks.
    Path filePath = new Path("/file1");
    DFSTestUtil.createFile(fs, filePath, BLOCK_LENGTH,
                           BLOCK_LENGTH * NUM_BLOCKS_PER_FILE,
                           BLOCK_LENGTH, INITIAL_REPLICAS, -1);

    // Start two more DataNodes with ARCHIVAL storage.
    LOG.info("Starting additional ARCHIVAL Datanodes");
    cluster.startDataNodesWithStorageType(conf, INITIAL_REPLICAS, StorageType.ARCHIVAL);
    cluster.waitActive();

    moveReplicasToStorageType(filePath, StorageType.ARCHIVAL, Short.MAX_VALUE);
    DFSTestUtil.waitReplicationForever(fs, filePath, INITIAL_REPLICAS);
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, INITIAL_REPLICAS);

    // Now shutdown an existing ARCHIVAL DN.
    shutdownOneDataNodeWithStorageType(StorageType.ARCHIVAL);

    // Ensure that no new replicas are created on DISK.
    Thread.sleep(120 * 1000);
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, INITIAL_REPLICAS - 1);
    assertThat(
        cluster.getNameNode().getNamesystem().getUnderReplicatedBlocks(),
        is((long) NUM_BLOCKS_PER_FILE));
  }

  /**
   * If a block has n > 1 replicas on StorageTypeA and m > 1 replicas on 
   * StorageType.DEFAULT, ensure that new replicas are placed on StorageTypeA.
   */
  @Test(timeout=600000)
  public void testAffinityToNonDefaultStorage()
      throws IOException, InterruptedException, TimeoutException {
    cluster.startDataNodesWithStorageType(conf, 1, StorageType.DEFAULT);
    cluster.waitActive();
    assertTrue(cluster.getDataNodes().size() >= 3);

    Path filePath = new Path("/file1");
    DFSTestUtil.createFile(fs, filePath, BLOCK_LENGTH,
                           BLOCK_LENGTH * NUM_BLOCKS_PER_FILE,
                           BLOCK_LENGTH, (short) 3, -1);

    // Start two more DataNodes with ARCHIVAL storage.
    LOG.info("Starting additional ARCHIVAL Datanodes");
    cluster.startDataNodesWithStorageType(conf, INITIAL_REPLICAS + 1, StorageType.ARCHIVAL);
    cluster.waitActive();

    // Move two replicas of each block to ARCHIVAL storage
    DatanodeInfo[] destinations = moveReplicasToStorageType(filePath, StorageType.ARCHIVAL, 2);
    DFSTestUtil.waitReplicationForever(fs, filePath, (short) 3);
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, 2);
    checkReplicaCountOnStorageType(filePath, StorageType.DEFAULT, 1);
    
    // Shutdown one of the ARCHIVAL DNs with a replica.
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getDatanodeUuid().equals(destinations[0].getDatanodeUuid())) {
        dn.shutdown();
      }
    }
    
    // Ensure that new replicas are created on the ARCHIVAL DN only.
    DFSTestUtil.waitReplicationForever(fs, filePath, (short) 3);
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, 2);
    checkReplicaCountOnStorageType(filePath, StorageType.DEFAULT, 1);    
  }

  @Test(timeout=600000)
  public void testIncreaseReplicationOfFileOnArchival()
      throws IOException, InterruptedException, TimeoutException {
    // Create a file with multiple blocks.
    Path filePath = new Path("/file1");
    DFSTestUtil.createFile(fs, filePath, BLOCK_LENGTH,
                           BLOCK_LENGTH * NUM_BLOCKS_PER_FILE,
                           BLOCK_LENGTH, INITIAL_REPLICAS, -1);

    // Start three more DataNodes with ARCHIVAL storage.
    LOG.info("Starting additional ARCHIVAL Datanodes");
    cluster.startDataNodesWithStorageType(conf, INITIAL_REPLICAS + 1, StorageType.ARCHIVAL);
    cluster.waitActive();

    moveReplicasToStorageType(filePath, StorageType.ARCHIVAL, Short.MAX_VALUE);
    DFSTestUtil.waitReplicationForever(fs, filePath, INITIAL_REPLICAS);
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, INITIAL_REPLICAS);

    // Increase the replication factor of the file and ensure new replicas
    // are all created on ARCHIVAL.
    fs.setReplication(filePath, (short) (INITIAL_REPLICAS + 1));
    DFSTestUtil.waitReplicationForever(fs, filePath, (short) (INITIAL_REPLICAS + 1));
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, INITIAL_REPLICAS + 1);

    // Increase the replication factor of the file once again and ensure new
    // replicas cannot be created since we don't have enough ARCHIVAL DNs.
    fs.setReplication(filePath, (short) (INITIAL_REPLICAS + 2));
    Thread.sleep(120 * 1000);
    checkReplicaCountOnStorageType(filePath, StorageType.ARCHIVAL, INITIAL_REPLICAS + 1);
    assertThat(
        cluster.getNameNode().getNamesystem().getUnderReplicatedBlocks(),
        is((long) NUM_BLOCKS_PER_FILE));
  }

  private void shutdownOneDataNodeWithStorageType(StorageType targetStorageType) {
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getFSDataset().getVolumes().get(0).getStorageType() == targetStorageType) {
        dn.shutdown();
        break;
      }
    }
  }

  /**
   * Move numReplicasToMove of the given file to the given target storage. If
   * numReplicasToMove is Short.MAX_VALUE then move all replicas.
   */
  private DatanodeInfo[] moveReplicasToStorageType(
      Path file, StorageType targetStorageType,
      int numReplicasToMove)
      throws IOException, InterruptedException, TimeoutException {
    // Build list of destination DataNodes.
    DatanodeInfo[] destinations = getMatchingDatanodes(targetStorageType, numReplicasToMove);

    // Move each replica of each block to a destination.
    LocatedBlocks locations = client.getLocatedBlocks(file.toString(), 0);
    for (LocatedBlock block : locations.getLocatedBlocks()) {
      // Ensure we have enough destination DNs to accommodate all the replicas.
      assertTrue("Too few destination Datanodes of target storage type. " +
                     " This is likely a test bug.",
                 destinations.length >= numReplicasToMove ||
                     destinations.length >= block.getLocations().length);

      // Move each replica to a different destination.
      for (int i = 0; i < block.getLocations().length && i < numReplicasToMove; ++i) {
        DatanodeInfo source = block.getLocations()[i];
        DatanodeInfo destination = destinations[i];
        LOG.info("Moving block " + block.getBlock().getBlockId() +
                     " from DN " + source.getDatanodeUuid() + " to " +
                     destination.getDatanodeUuid());
        boolean replaceResult = replaceBlock(block.getBlock(), source, source, destination);
        assertThat(replaceResult, is(true));
      }
    }

    for (DataNode dn : cluster.getDataNodes()) {
      DataNodeTestUtils.triggerDeletionReport(dn);
      DataNodeTestUtils.triggerBlockReport(dn);
    }
    
    return destinations;
  }
  
  private DatanodeInfo[] getMatchingDatanodes(StorageType targetStorageType, int count) {
    List<DatanodeInfo> destinations = new ArrayList<DatanodeInfo>();
    for (DataNode dn : cluster.getDataNodes()) {
      if (dn.getFSDataset().getVolumes().get(0).getStorageType() == targetStorageType) {
        LOG.info("Chose target " + dn.getDatanodeUuid());
        destinations.add(new DatanodeInfo(dn.getDatanodeId()));
      }
      
      if (destinations.size() == count) {
        break;
      }
    }
    return destinations.toArray(new DatanodeInfo[destinations.size()]);
  }

  private boolean replaceBlock(ExtendedBlock block, DatanodeInfo source,
                               DatanodeInfo sourceProxy,
                               DatanodeInfo destination) throws IOException {

    Socket sock = new Socket();
    sock.connect(NetUtils.createSocketAddr(
        destination.getXferAddr()), HdfsServerConstants.READ_TIMEOUT);
    sock.setKeepAlive(true);
    // sendRequest
    DataOutputStream out = new DataOutputStream(sock.getOutputStream());
    new Sender(out).replaceBlock(block, BlockTokenSecretManager.DUMMY_TOKEN,
                                 source.getDatanodeUuid(), sourceProxy);
    out.flush();
    // receiveResponse
    DataInputStream reply = new DataInputStream(sock.getInputStream());

    DataTransferProtos.BlockOpResponseProto proto =
        DataTransferProtos.BlockOpResponseProto.parseDelimitedFrom(reply);
    return proto.getStatus() == DataTransferProtos.Status.SUCCESS;
  }

  @Test(timeout=600000)
  public void testDecommission() {
    // To be written.
  }
}