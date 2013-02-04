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

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

public class INodeFile extends INode {
  /** Cast INode to INodeFile. */
  public static INodeFile valueOf(INode inode, String path)
      throws FileNotFoundException {
    if (inode == null) {
      throw new FileNotFoundException("File does not exist: " + path);
    }
    if (!(inode instanceof INodeFile)) {
      throw new FileNotFoundException("Path is not a file: " + path);
    }
    return (INodeFile)inode;
  }

  static final FsPermission UMASK = FsPermission.createImmutable((short)0111);

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

  INodeFile(PermissionStatus permissions,
            int nrBlocks, short replication, long modificationTime,
            long atime, long preferredBlockSize) {
    this(permissions, new BlockInfo[nrBlocks], replication,
        modificationTime, atime, preferredBlockSize);
  }

  protected INodeFile(PermissionStatus permissions, BlockInfo[] blklist,
                      short replication, long modificationTime,
                      long atime, long preferredBlockSize) {
    this(null, permissions, modificationTime, atime, blklist, replication,
        preferredBlockSize);
  }
  
  INodeFile(byte[] name, PermissionStatus permissions, long mtime, long atime,
      BlockInfo[] blklist, short replication, long preferredBlockSize) {
    super(name, permissions, null, mtime, atime);
    header = HeaderFormat.combineReplication(header, replication);
    header = HeaderFormat.combinePreferredBlockSize(header, preferredBlockSize);
    this.blocks = blklist;
  }

  /** @return true unconditionally. */
  @Override
  public final boolean isFile() {
    return true;
  }
  
  /**
   * Set the {@link FsPermission} of this {@link INodeFile}.
   * Since this is a file,
   * the {@link FsAction#EXECUTE} action, if any, is ignored.
   */
  @Override
  INode setPermission(FsPermission permission, Snapshot latest) {
    return super.setPermission(permission.applyUMask(UMASK), latest);
  }

  protected INodeFile(INodeFile that) {
    super(that);
    this.header = that.header;
    this.blocks = that.blocks;
  }

  @Override
  INodeFile recordModification(final Snapshot latest) {
    //TODO: change it to use diff list
    return (INodeFile)super.recordModification(latest);
  }

  @Override
  public Pair<? extends INodeFile, ? extends INodeFile> createSnapshotCopy() {
    return parent.replaceINodeFile(this).createSnapshotCopy();
  }

  /** @return the replication factor of the file. */
  public final short getFileReplication() {
    return HeaderFormat.getReplication(header);
  }

  public short getBlockReplication() {
    return getFileReplication();
  }

  public void setFileReplication(short replication, Snapshot latest) {
    if (latest != null) {
      recordModification(latest).setFileReplication(replication, null);
      return;
    }

    header = HeaderFormat.combineReplication(header, replication);
  }

  /** Convert this file to an {@link INodeFileUnderConstruction}. */
  public INodeFileUnderConstruction toUnderConstruction(
      String clientName,
      String clientMachine,
      DatanodeDescriptor clientNode) {
    if (this instanceof INodeFileUnderConstruction) {
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

  /**
   * Get file blocks 
   * @return file blocks
   */
  public BlockInfo[] getBlocks() {
    return this.blocks;
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
  public int collectSubtreeBlocksAndClear(BlocksMapUpdateInfo info) {
    parent = null;
    if (blocks != null && info != null) {
      for (BlockInfo blk : blocks) {
        info.addDeleteBlock(blk);
        blk.setINode(null);
      }
    }
    setBlocks(null);
    return 1;
  }

  @Override
  long[] computeContentSummary(long[] summary) {
    summary[0] += computeFileSize();
    summary[1]++;
    summary[3] += diskspaceConsumed();
    return summary;
  }
  
  /** 
   * Compute file size.
   */
  public long computeFileSize() {
    if (blocks == null || blocks.length == 0) {
      return 0;
    }
    long bytes = 0;
    for(int i = 0; i < blocks.length; i++) {
      bytes += blocks[i].getNumBytes();
    }
    return bytes;
  }

  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    counts.dsCount += diskspaceConsumed();
    return counts;
  }

  long diskspaceConsumed() {
    return diskspaceConsumed(blocks);
  }
  
  long diskspaceConsumed(Block[] blkArr) {
    long size = 0;
    for (Block blk : blkArr) {
      if (blk != null) {
        size += blk.getNumBytes();
      }
    }
    /* If the last block is being written to, use prefferedBlockSize
     * rather than the actual block size.
     */
    if (blkArr.length > 0 && blkArr[blkArr.length-1] != null && 
        isUnderConstruction()) {
      size += getPreferredBlockSize() - blocks[blocks.length-1].getNumBytes();
    }
    return size * getFileReplication();
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
  Block getLastBlock() {
    if (this.blocks == null || this.blocks.length == 0)
      return null;
    return this.blocks[this.blocks.length - 1];
  }
}
