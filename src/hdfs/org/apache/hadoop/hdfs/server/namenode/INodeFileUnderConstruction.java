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

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;


public class INodeFileUnderConstruction extends INodeFile {
  /** Cast INode to INodeFileUnderConstruction. */
  public static INodeFileUnderConstruction valueOf(INode inode, String path
      ) throws FileNotFoundException {
    final INodeFile file = INodeFile.valueOf(inode, path);
    if (!file.isUnderConstruction()) {
      throw new FileNotFoundException("File is not under construction: " + path);
    }
    return (INodeFileUnderConstruction)file;
  }

  String clientName;         // lease holder
  private final String clientMachine;
  private final DatanodeDescriptor clientNode; // if client is a cluster node too.

  private int primaryNodeIndex = -1; //the node working on lease recovery
  private DatanodeDescriptor[] targets = null;   //locations for last block
  private long lastRecoveryTime = 0;
  
  INodeFileUnderConstruction(PermissionStatus permissions,
                             short replication,
                             long preferredBlockSize,
                             long modTime,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode) {
    this(null, replication, modTime, preferredBlockSize, BlockInfo.EMPTY_ARRAY,
        permissions.applyUMask(UMASK), clientName, clientMachine, clientNode);
  }

  public INodeFileUnderConstruction(byte[] name,
                             short blockReplication,
                             long modificationTime,
                             long preferredBlockSize,
                             BlockInfo[] blocks,
                             PermissionStatus perm,
                             String clientName,
                             String clientMachine,
                             DatanodeDescriptor clientNode) {
    super(name, perm, modificationTime, modificationTime, blocks,
        blockReplication, preferredBlockSize);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }
  
  public INodeFileUnderConstruction(final INodeFile that,
      final String clientName,
      final String clientMachine,
      final DatanodeDescriptor clientNode) {
    super(that);
    this.clientName = clientName;
    this.clientMachine = clientMachine;
    this.clientNode = clientNode;
  }

  public String getClientName() {
    return clientName;
  }

  void setClientName(String newName) {
    clientName = newName;
  }

  public String getClientMachine() {
    return clientMachine;
  }

  public DatanodeDescriptor getClientNode() {
    return clientNode;
  }

  /**
   * Is this inode being constructed?
   */
  @Override
  boolean isUnderConstruction() {
    return true;
  }
  
  @Override
  public INodeFileUnderConstruction recordModification(final Snapshot latest)
      throws NSQuotaExceededException {
    return isInLatestSnapshot(latest) ? parent
        .replaceChild4INodeFileUcWithSnapshot(this).recordModification(latest)
        : this;
  }

  DatanodeDescriptor[] getTargets() {
    return targets;
  }

  void setTargets(DatanodeDescriptor[] targets) {
    this.targets = targets;
    this.primaryNodeIndex = -1;
  }

  /**
   * add this target if it does not already exists
   */
  void addTarget(DatanodeDescriptor node) {
    if (this.targets == null) {
      this.targets = new DatanodeDescriptor[0];
    }

    for (int j = 0; j < this.targets.length; j++) {
      if (this.targets[j].equals(node)) {
        return;  // target already exists
      }
    }
      
    // allocate new data structure to store additional target
    DatanodeDescriptor[] newt = new DatanodeDescriptor[targets.length + 1];
    for (int i = 0; i < targets.length; i++) {
      newt[i] = this.targets[i];
    }
    newt[targets.length] = node;
    this.targets = newt;
    this.primaryNodeIndex = -1;
  }

   /**
    * Converts an INodeFileUnderConstruction to an INodeFile.
    * The original modification time is used as the access time.
    * The new modification is the specified mtime.
    */
   protected INodeFile toINodeFile(long mtime) {
    final INodeFile f = new INodeFile(getLocalNameBytes(),
        getPermissionStatus(), mtime, getModificationTime(), getBlocks(),
        getFileReplication(), getPreferredBlockSize());
    f.setParent(getParent());
    return f;
  }

  /**
   * remove a block from the block list. This block should be
   * the last one on the list.
   */
  void removeBlock(Block oldblock) throws IOException {
    final BlockInfo[] blocks = getBlocks();
    if (blocks == null) {
      throw new IOException("Trying to delete non-existant block " + oldblock);
    }
    int size_1 = blocks.length - 1;
    if (!blocks[size_1].equals(oldblock)) {
      throw new IOException("Trying to delete non-last block " + oldblock);
    }

    //copy to a new list
    BlockInfo[] newlist = new BlockInfo[size_1];
    System.arraycopy(blocks, 0, newlist, 0, size_1);
    setBlocks(newlist);
    
    // Remove the block locations for the last block.
    targets = null;
  }

  synchronized void setLastBlock(BlockInfo newblock,
      DatanodeDescriptor[] newtargets) throws IOException {
    final BlockInfo[] blocks = getBlocks();
    if (blocks == null || blocks.length == 0) {
      throw new IOException("Trying to update non-existant block (newblock="
          + newblock + ")");
    }
    BlockInfo oldLast = blocks[blocks.length - 1];
    if (oldLast.getBlockId() != newblock.getBlockId()) {
      // This should not happen - this means that we're performing recovery
      // on an internal block in the file!
      NameNode.stateChangeLog.error(
        "Trying to commit block synchronization for an internal block on"
        + " inode=" + this
        + " newblock=" + newblock + " oldLast=" + oldLast);
      throw new IOException("Trying to update an internal block of " +
                            "pending file " + this);
    }

    if (oldLast.getGenerationStamp() > newblock.getGenerationStamp()) {
      NameNode.stateChangeLog.warn(
        "Updating last block " + oldLast + " of inode " +
        "under construction " + this + " with a block that " +
        "has an older generation stamp: " + newblock);
    }

    blocks[blocks.length - 1] = newblock;
    setTargets(newtargets);
    lastRecoveryTime = 0;
  }

  /**
   * Initialize lease recovery for this object
   */
  void assignPrimaryDatanode() {
    //assign the first alive datanode as the primary datanode

    if (targets.length == 0) {
      NameNode.stateChangeLog.warn("BLOCK*"
        + " INodeFileUnderConstruction.initLeaseRecovery:"
        + " No blocks found, lease removed.");
    }

    int previous = primaryNodeIndex;
    //find an alive datanode beginning from previous
    for(int i = 1; i <= targets.length; i++) {
      int j = (previous + i)%targets.length;
      if (targets[j].isAlive) {
        DatanodeDescriptor primary = targets[primaryNodeIndex = j]; 
        final BlockInfo[] blocks = getBlocks();
        primary.addBlockToBeRecovered(blocks[blocks.length - 1], targets);
        NameNode.stateChangeLog.info("BLOCK* " + blocks[blocks.length - 1]
          + " recovery started, primary=" + primary);
        return;
      }
    }
  }
  
  /**
   * Update lastRecoveryTime if expired.
   * @return true if lastRecoveryTimeis updated. 
   */
  synchronized boolean setLastRecoveryTime(long now) {
    boolean expired = now - lastRecoveryTime > NameNode.LEASE_RECOVER_PERIOD;
    if (expired) {
      lastRecoveryTime = now;
    }
    return expired;
  }
  
  /**
   * Update the length for the last block
   * 
   * @param lastBlockLength
   *          The length of the last block reported from client
   * @throws IOException
   */
  void updateLengthOfLastBlock(long lastBlockLength) throws IOException {
    final BlockInfo[] blocks = getBlocks();
    assert blocks != null : "Blocks for path " + this.getFullPathName()
        + " is null when updating its last block length";
    BlockInfo lastBlock = blocks[blocks.length - 1];
    assert (lastBlock != null) : "The last block for path "
        + this.getFullPathName() + " is null when updating its length";
    lastBlock.setNumBytes(lastBlockLength);
  }
}
