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
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.Content.CountsMap.Key;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot.FileDiff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot.Util;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

public class INodeFile extends INode {
  /** The same as valueOf(inode, path, false). */
  public static INodeFile valueOf(INode inode, String path
      ) throws FileNotFoundException {
    return valueOf(inode, path, false);
  }

  /** Cast INode to INodeFile. */
  public static INodeFile valueOf(INode inode, String path, boolean acceptNull)
      throws FileNotFoundException {
    if (inode == null) {
      if (acceptNull) {
        return null;
      } else {
        throw new FileNotFoundException("File does not exist: " + path);
      }
    }
    if (!inode.isFile()) {
      throw new FileNotFoundException("Path is not a file: " + path);
    }
    return inode.asFile();
  }

  /** Format: [16 bits for replication][48 bits for PreferredBlockSize] */
  private static class HeaderFormat {
    /** Number of bits for Block size */
    static final int BLOCKBITS = 48;
    /** Header mask 64-bit representation */
    static final long HEADERMASK = 0xffffL << BLOCKBITS;
    static final long MAX_BLOCK_SIZE = ~HEADERMASK; 
    
    static short getReplication(long header) {
      return (short) ((header & HEADERMASK) >> BLOCKBITS);
    }

    static long combineReplication(long header, short replication) {
      if (replication <= 0) {
         throw new IllegalArgumentException(
             "Unexpected value for the replication: " + replication);
      }
      return ((long)replication << BLOCKBITS) | (header & MAX_BLOCK_SIZE);
    }
    
    static long getPreferredBlockSize(long header) {
      return header & MAX_BLOCK_SIZE;
    }

    static long combinePreferredBlockSize(long header, long blockSize) {
      if (blockSize < 0) {
         throw new IllegalArgumentException("Block size < 0: " + blockSize);
      } else if (blockSize > MAX_BLOCK_SIZE) {
        throw new IllegalArgumentException("Block size = " + blockSize
            + " > MAX_BLOCK_SIZE = " + MAX_BLOCK_SIZE);
     }
      return (header & HEADERMASK) | (blockSize & MAX_BLOCK_SIZE);
    }
  }

  protected long header = 0L;

  private BlockInfo blocks[] = null;
  
  INodeFile(byte[] name, PermissionStatus permissions, long mtime, long atime,
      BlockInfo[] blklist, short replication, long preferredBlockSize) {
    super(name, permissions, mtime, atime);
    header = HeaderFormat.combineReplication(header, replication);
    header = HeaderFormat.combinePreferredBlockSize(header, preferredBlockSize);
    this.blocks = blklist;
  }
  
  public INodeFile(INodeFile that) {
    super(that);
    this.header = that.header;
    this.blocks = that.blocks;
  }

  /** @return true unconditionally. */
  @Override
  public final boolean isFile() {
    return true;
  }
  
  @Override
  public INodeFile getSnapshotINode(final Snapshot snapshot) {
    return this;
  }

  @Override
  public INodeFile recordModification(final Snapshot latest)
      throws QuotaExceededException {
    return isInLatestSnapshot(latest)?
        getParent().replaceChild4INodeFileWithSnapshot(this)
            .recordModification(latest)
        : this;
  }
  
  /**
   * Set the {@link FsPermission} of this {@link INodeFile}.
   * Since this is a file,
   * the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  @Override
  final void setPermission(FsPermission permission) {
    super.setPermission(permission);
  }

  /**
   * Set the {@link FsPermission} of this {@link INodeFile}.
   * Since this is a file,
   * the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  @Override
  final INode setPermission(FsPermission permission, Snapshot latest)
      throws QuotaExceededException {
    return super.setPermission(permission, latest);
  }

  /** @return the replication factor of the file. */
  public final short getFileReplication(Snapshot snapshot) {
    if (snapshot != null) {
      return getSnapshotINode(snapshot).getFileReplication();
    }

    return HeaderFormat.getReplication(header);
  }
  
  /** The same as getFileReplication(null). */
  public final short getFileReplication() {
    return getFileReplication(null);
  }

  public final short getBlockReplication() {
    return this instanceof FileWithSnapshot?
        Util.getBlockReplication((FileWithSnapshot)this)
        : getFileReplication(null);
  }

  /** Set the replication factor of this file. */
  public final void setFileReplication(short replication) {
    header = HeaderFormat.combineReplication(header, replication);
  }

  /** Set the replication factor of this file. */
  public final INodeFile setFileReplication(short replication, Snapshot latest)
      throws QuotaExceededException {
    final INodeFile nodeToUpdate = recordModification(latest);
    nodeToUpdate.setFileReplication(replication);
    return nodeToUpdate;
  }
  
  /** @return this object. */
  @Override
  public final INodeFile asFile() {
    return this;
  }

  /** Is this file under construction? */
  public boolean isUnderConstruction() {
    return false;
  }

  /** Convert this file to an {@link INodeFileUnderConstruction}. */
  public INodeFileUnderConstruction toUnderConstruction(
      String clientName,
      String clientMachine,
      DatanodeDescriptor clientNode) {
    if (isUnderConstruction()) {
      throw new IllegalStateException(
          "file is already an INodeFileUnderConstruction");
    }
    return new INodeFileUnderConstruction(this,
        clientName, clientMachine, clientNode); 
  }

  /**
   * Get preferred block size for the file
   * @return preferred block size in bytes
   */
  public long getPreferredBlockSize() {
    return HeaderFormat.getPreferredBlockSize(header);
  }

  /** @return the diskspace required for a full block. */
  final long getBlockDiskspace() {
    return getPreferredBlockSize() * getBlockReplication();
  }
  
  /**
   * Get file blocks 
   * @return file blocks
   */
  public BlockInfo[] getBlocks() {
    return this.blocks;
  }
  
  public int numBlocks() {
    return blocks == null ? 0 : blocks.length;
  }
  
  void updateINodeForBlocks() {
    if (blocks != null) {
      for(BlockInfo b : blocks) {
        b.setINode(this);
      }
    }
  }

  /**
   * append array of blocks to this.blocks
   */
  void concatBlocks(INodeFile[] inodes) {
    int size = this.blocks.length;
    int totalAddedBlocks = 0;
    for(INodeFile f : inodes) {
      totalAddedBlocks += f.blocks.length;
    }
    
    BlockInfo[] newlist = new BlockInfo[size + totalAddedBlocks];
    System.arraycopy(this.blocks, 0, newlist, 0, size);
    
    for(INodeFile in: inodes) {
      System.arraycopy(in.blocks, 0, newlist, size, in.blocks.length);
      size += in.blocks.length;
    }

    setBlocks(newlist);
    updateINodeForBlocks();
  }
  
  /**
   * add a block to the block list
   */
  void addBlock(BlockInfo newblock) {
    if (this.blocks == null) {
      this.setBlocks(new BlockInfo[]{newblock});
    } else {
      int size = this.blocks.length;
      BlockInfo[] newlist = new BlockInfo[size + 1];
      System.arraycopy(this.blocks, 0, newlist, 0, size);
      newlist[size] = newblock;
      setBlocks(newlist);
    }
  }

  /**
   * Set file block
   */
  void setBlock(int idx, BlockInfo blk) {
    this.blocks[idx] = blk;
  }
  
  /** Set the blocks. */
  public void setBlocks(BlockInfo[] blocks) {
    this.blocks = blocks;
  }

  @Override
  public Quota.Counts cleanSubtree(final Snapshot snapshot, Snapshot prior,
      final BlocksMapUpdateInfo collectedBlocks)
      throws QuotaExceededException {
    Quota.Counts counts = Quota.Counts.newInstance();
    if (snapshot == null && prior == null) {   
      // this only happens when deleting the current file
      computeQuotaUsage(counts, false);
      destroyAndCollectBlocks(collectedBlocks);
    }
    return counts;
  }

  @Override
  public void destroyAndCollectBlocks(BlocksMapUpdateInfo collectedBlocks) {
    if (blocks != null && collectedBlocks != null) {
      for (BlockInfo blk : blocks) {
        collectedBlocks.addDeleteBlock(blk);
        blk.setINode(null);
      }
    }
    setBlocks(null);
    clearReferences();
    if (this instanceof FileWithSnapshot) {
      ((FileWithSnapshot) this).getDiffs().clear();
    }
  }

  LocatedBlocks createLocatedBlocks(List<LocatedBlock> blocks, 
      Snapshot snapshot) {
    return new LocatedBlocks(computeFileSize(snapshot), blocks,
        isUnderConstruction());
  }
  
  @Override
  public final Quota.Counts computeQuotaUsage(Quota.Counts counts,
      boolean useCache) {
    counts.add(Quota.NAMESPACE, this instanceof FileWithSnapshot?
        ((FileWithSnapshot)this).getDiffs().asList().size() + 1: 1);
    counts.add(Quota.DISKSPACE, diskspaceConsumed());
    return counts;
  }

  @Override
  public final Content.CountsMap computeContentSummary(
      final Content.CountsMap countsMap) {
    computeContentSummary4Snapshot(countsMap.getCounts(Key.SNAPSHOT));
    computeContentSummary4Current(countsMap.getCounts(Key.CURRENT));
    return countsMap;
  }

  @Override
  public final Content.Counts computeContentSummary(
      final Content.Counts counts) {
    computeContentSummary4Snapshot(counts);
    computeContentSummary4Current(counts);
    return counts;
  }

  private void computeContentSummary4Snapshot(final Content.Counts counts) {
    // file length and diskspace only counted for the latest state of the file
    // i.e. either the current state or the last snapshot
    if (this instanceof FileWithSnapshot) {
      final FileWithSnapshot withSnapshot = (FileWithSnapshot)this;
      final FileDiffList diffs = withSnapshot.getDiffs();
      final int n = diffs.asList().size();
      counts.add(Content.FILE, n);
      if (n > 0 && withSnapshot.isCurrentFileDeleted()) {
        counts.add(Content.LENGTH, diffs.getLast().getFileSize());
      }

      if (withSnapshot.isCurrentFileDeleted()) {
        final long lastFileSize = diffs.getLast().getFileSize();
        counts.add(Content.DISKSPACE, lastFileSize * getBlockReplication());
      }
    }
  }

  private void computeContentSummary4Current(final Content.Counts counts) {
    if (this instanceof FileWithSnapshot
        && ((FileWithSnapshot)this).isCurrentFileDeleted()) {
      return;
    }

    counts.add(Content.LENGTH, computeFileSize());
    counts.add(Content.FILE, 1);
    counts.add(Content.DISKSPACE, diskspaceConsumed());
  }

  /**
   * Compute file size of the current file if the given snapshot is null;
   * otherwise, get the file size from the given snapshot.
   */
  public final long computeFileSize(Snapshot snapshot) {
    if (snapshot != null && this instanceof FileWithSnapshot) {
      final FileDiff d = ((FileWithSnapshot)this).getDiffs().getDiff(snapshot);
      if (d != null) {
        return d.getFileSize();
      }
    }

    return computeFileSize();
  }

  /**
   * Compute file size of the current file.
   * @return file size
   */
  public final long computeFileSize() {
    if (blocks == null || blocks.length == 0) {
      return 0;
    }
    long size = 0;
    //sum other blocks
    for(int i = 0; i < blocks.length; i++) {
      size += blocks[i].getNumBytes();
    }
    return size;
  }

  public final long diskspaceConsumed() {
    long size = computeFileSize();
    /* If the last block is being written to, use prefferedBlockSize
     * rather than the actual block size.
     */
    if (blocks != null && blocks.length > 0
        && blocks[blocks.length - 1] != null && isUnderConstruction()) {
      size += getPreferredBlockSize() - blocks[blocks.length-1].getNumBytes();
    }
    
    // use preferred block size for the last block if it is under construction
    return size * getBlockReplication();
  }
  
  /**
   * Return the penultimate allocated block for this file.
   */
  Block getPenultimateBlock() {
    if (blocks == null || blocks.length <= 1) {
      return null;
    }
    return blocks[blocks.length - 2];
  }

  INodeFileUnderConstruction toINodeFileUnderConstruction(
      String clientName, String clientMachine, DatanodeDescriptor clientNode
      ) throws IOException {
    if (isUnderConstruction()) {
      return (INodeFileUnderConstruction)this;
    }
    return new INodeFileUnderConstruction(this.getLocalNameBytes(),
        getBlockReplication(), modificationTime, getPreferredBlockSize(),
        blocks, getPermissionStatus(),
        clientName, clientMachine, clientNode);
  }

  /**
   * Return the last block in this file, or null if there are no blocks.
   */
  BlockInfo getLastBlock() {
    if (this.blocks == null || this.blocks.length == 0)
      return null;
    return this.blocks[this.blocks.length - 1];
  }
  
  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final Snapshot snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);
    out.print(", fileSize=" + computeFileSize(snapshot));
    // only compare the first block
    out.print(", blocks=");
    out.print(blocks == null || blocks.length == 0 ? null : blocks[0]);
    out.println();
  }
}
