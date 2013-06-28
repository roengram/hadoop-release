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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.SnapshotTestHelper;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test snapshot functionalities while file appending.
 */
public class TestINodeFileUnderConstructionWithSnapshot {
  {
    ((Log4JLogger)INode.LOG).getLogger().setLevel(Level.ALL);
    SnapshotTestHelper.disableLogs();
  }

  static final long seed = 0;
  static final short REPLICATION = 3;
  static final int BLOCKSIZE = 1024;

  private final Path dir = new Path("/TestSnapshot");
  
  Configuration conf;
  MiniDFSCluster cluster;
  FSNamesystem fsn;
  DistributedFileSystem hdfs;
  FSDirectory fsdir;
  
  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCKSIZE);
    conf.setBoolean("dfs.support.broken.append", true);
    cluster = new MiniDFSCluster(conf, REPLICATION, true, null);
    cluster.waitActive();
    fsn = cluster.getNameNode().getNamesystem();
    fsdir = fsn.getFSDirectory();
    hdfs = (DistributedFileSystem) cluster.getFileSystem();
    hdfs.mkdirs(dir);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  private FSDataOutputStream writeFileWithoutClosing(Path file, int length)
      throws IOException {
    byte[] toWrite = new byte[length];
    Random random = new Random();
    random.nextBytes(toWrite);
    FSDataOutputStream out = hdfs.create(file);
    out.write(toWrite);
    return out;
  }
  
  /**
   * Test snapshot during file appending, before the corresponding
   * {@link FSDataOutputStream} instance closes.
   */
  @Test
  public void testSnapshotWhileWriting() throws Exception {
    Path file = new Path(dir, "file");
    // create without closing stream --> create snapshot
    FSDataOutputStream out = writeFileWithoutClosing(file, BLOCKSIZE);
    ((DFSOutputStream) out.getWrappedStream()).sync(true);
    SnapshotTestHelper.createSnapshot(hdfs, dir, "s0");
    
    // close the stream
    out.close();
    
    // check: an INodeFileUnderConstructionSnapshot should be stored into s0's
    // deleted list, with size BLOCKSIZE
    INodeFile fileNode = (INodeFile) fsdir.getINode(file.toString());
    assertEquals(BLOCKSIZE, fileNode.computeFileSize());
    INodeDirectorySnapshottable dirNode = (INodeDirectorySnapshottable) fsdir
        .getINode(dir.toString());
    Snapshot s0 = dirNode.getDiffs().getLast().snapshot;
    assertEquals(BLOCKSIZE, fileNode.computeFileSize(s0));
  }
  
  private LocatedBlocks callGetBlockLocations(String src, long offset,
      long length) throws IOException {
    return hdfs.getClient().namenode.getBlockLocations(src, offset, length);
  }
  
  private FSDataOutputStream appendFileWithoutClosing(Path file, int length)
      throws IOException {
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    FSDataOutputStream out = hdfs.append(file);
    out.write(toAppend);
    return out;
  }
  
  /**
   * call DFSClient#callGetBlockLocations(...) for snapshot file. Make sure only
   * blocks within the size range are returned.
   */
  @Test
  public void testGetBlockLocations() throws Exception {
    final Path root = new Path("/");
    final Path file = new Path("/file");
    DFSTestUtil.createFile(hdfs, file, BLOCKSIZE, REPLICATION, seed);
    
    // take a snapshot on root
    SnapshotTestHelper.createSnapshot(hdfs, root, "s1");
    
    final Path fileInSnapshot = SnapshotTestHelper.getSnapshotPath(root,
        "s1", file.getName());
    FileStatus status = hdfs.getFileStatus(fileInSnapshot);
    // make sure we record the size for the file
    assertEquals(BLOCKSIZE, status.getLen());
    
    // append data to file
    DFSTestUtil.appendFile(hdfs, file, BLOCKSIZE - 1);
    status = hdfs.getFileStatus(fileInSnapshot);
    // the size of snapshot file should still be BLOCKSIZE
    assertEquals(BLOCKSIZE, status.getLen());
    // the size of the file should be (2 * BLOCKSIZE - 1)
    status = hdfs.getFileStatus(file);
    assertEquals(BLOCKSIZE * 2 - 1, status.getLen());
    
    // invoke callGetBlockLocations for the file in snapshot
    LocatedBlocks blocks = callGetBlockLocations(fileInSnapshot.toString(), 0,
        Long.MAX_VALUE);
    assertFalse(blocks.isUnderConstruction());
    assertEquals(BLOCKSIZE, blocks.getFileLength());
    
    List<LocatedBlock> blockList = blocks.getLocatedBlocks();
    // should be only one block
    assertEquals(BLOCKSIZE, blocks.getFileLength());
    assertEquals(1, blockList.size());
    
    // take another snapshot
    SnapshotTestHelper.createSnapshot(hdfs, root, "s2");
    final Path fileInSnapshot2 = SnapshotTestHelper.getSnapshotPath(root,
        "s2", file.getName());
    
    // append data to file without closing
    FSDataOutputStream out = appendFileWithoutClosing(file, BLOCKSIZE);
    ((DFSOutputStream) out.getWrappedStream()).sync(true);
    
    status = hdfs.getFileStatus(fileInSnapshot2);
    // the size of snapshot file should be BLOCKSIZE*2-1
    assertEquals(BLOCKSIZE * 2 - 1, status.getLen());
    // the size of the file should be (3 * BLOCKSIZE - 1)
    status = hdfs.getFileStatus(file);
    assertEquals(BLOCKSIZE * 3 - 1, status.getLen());
    
    blocks = callGetBlockLocations(fileInSnapshot2.toString(), 0,
        Long.MAX_VALUE);
    assertFalse(blocks.isUnderConstruction());
    blockList = blocks.getLocatedBlocks();
    
    // should be 2 blocks
    assertEquals(BLOCKSIZE * 2 - 1, blocks.getFileLength());
    assertEquals(2, blockList.size());
    
    blocks = callGetBlockLocations(fileInSnapshot2.toString(), BLOCKSIZE, 0);
    blockList = blocks.getLocatedBlocks();
    assertEquals(1, blockList.size());
    
    // check blocks for file being written
    blocks = callGetBlockLocations(file.toString(), 0, Long.MAX_VALUE);
    blockList = blocks.getLocatedBlocks();
    assertEquals(3, blockList.size());
    assertTrue(blocks.isUnderConstruction());
    assertEquals(BLOCKSIZE * 3 - 1, blocks.getFileLength());
    
    out.close();
  }
}