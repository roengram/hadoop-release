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

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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
}