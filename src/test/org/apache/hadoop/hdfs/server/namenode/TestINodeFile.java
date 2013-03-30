/**
SE * Licensed to the Apache Software Foundation (ASF) under one
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.ExtendedDirectoryListing;
import org.apache.hadoop.hdfs.protocol.ExtendedHdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.junit.Test;
import org.mockito.Mockito;

public class TestINodeFile {

  static final short BLOCKBITS = 48;
  static final long BLKSIZE_MAXVALUE = ~(0xffffL << BLOCKBITS);

  private final PermissionStatus perm = new PermissionStatus(
      "userName", null, FsPermission.getDefault());
  private short replication;
  private long preferredBlockSize;

  INodeFile createINodeFile(short replication, long preferredBlockSize) {
    return new INodeFile(INodeId.GRANDFATHER_INODE_ID, null, perm, 0L, 0L,
        null, replication, preferredBlockSize);
  }
  /**
   * Test for the Replication value. Sets a value and checks if it was set
   * correct.
   */
  @Test
  public void testReplication () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", replication,
                 inf.getFileReplication());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for Replication.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testReplicationBelowLowerBound ()
              throws IllegalArgumentException {
    replication = -1;
    preferredBlockSize = 128*1024*1024;
    createINodeFile(replication, preferredBlockSize);
  }

  /**
   * Test for the PreferredBlockSize value. Sets a value and checks if it was
   * set correct.
   */
  @Test
  public void testPreferredBlockSize () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", preferredBlockSize,
           inf.getPreferredBlockSize());
  }

  @Test
  public void testPreferredBlockSizeUpperBound () {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE;
    INodeFile inf = createINodeFile(replication, preferredBlockSize);
    assertEquals("True has to be returned in this case", BLKSIZE_MAXVALUE,
                 inf.getPreferredBlockSize());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeBelowLowerBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = -1;
    createINodeFile(replication, preferredBlockSize);
  } 

  /**
   * IllegalArgumentException is expected for setting above upper bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeAboveUpperBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE+1;
    createINodeFile(replication, preferredBlockSize);
  }


  /**
   * Test for the static {@link INodeFile#valueOf(INode, String)}
   * and {@link INodeFileUnderConstruction#valueOf(INode, String)} methods.
   * @throws IOException 
   */
  @Test
  public void testValueOf () throws IOException {
    final String path = "/testValueOf";
    final short replication = 3;

    {//cast from null
      final INode from = null;

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("File does not exist"));
      }

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("File does not exist"));
      }

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch (FileNotFoundException e) {
        assertTrue(e.getMessage().contains("Directory does not exist"));
      }
    }

    {//cast from INodeFile
      final INode from = createINodeFile(replication, preferredBlockSize);
      
      //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch(IOException ioe) {
        assertTrue(ioe.getMessage().contains("File is not under construction"));
      }

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(FileNotFoundException e) {
        assertTrue(e.getMessage().contains("Is not a directory"));
      }
    }

    {//cast from INodeFileUnderConstruction
      final INode from = new INodeFileUnderConstruction(
          INodeId.GRANDFATHER_INODE_ID, perm, replication, 0L, 0L, "client",
          "machine", null);
      
      //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeFileUnderConstruction, should success
      final INodeFileUnderConstruction u = INodeFileUnderConstruction.valueOf(
          from, path);
      assertTrue(u == from);

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(FileNotFoundException e) {
        assertTrue(e.getMessage().contains("Is not a directory"));
      }
    }

    {//cast from INodeDirectory
      final INode from = new INodeDirectory(INodeId.GRANDFATHER_INODE_ID, null,
          perm, 0L);

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("Path is not a file"));
      }

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("Path is not a file"));
      }

      //cast to INodeDirectory, should success
      final INodeDirectory d = INodeDirectory.valueOf(from, path);
      assertTrue(d == from);
    }
  }
  
  /**
   * This test verifies inode ID counter and inode map functionality.
   */
  @Test
  public void testInodeId() throws IOException {

    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_SUPPORT_FILEID_KEY, true);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(0, conf, 1, true, true, null, null);
      cluster.waitActive();
  
      FSNamesystem fsn = FSNamesystem.getFSNamesystem();
      long lastId = fsn.getLastInodeId();
      
      // Ensure root has the correct inode ID
      // Last inode ID should be root inode ID and inode map size should be 1
      int inodeCount = 1;
      long expectedLastInodeId = INodeId.ROOT_INODE_ID;
      assertEquals(fsn.dir.rootDir.getId(), INodeId.ROOT_INODE_ID);
      assertEquals(expectedLastInodeId, lastId);
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
  
      // Create a directory
      // Last inode ID and inode map size should increase by 1
      FileSystem fs = cluster.getFileSystem();
      Path path = new Path("/test1");
      assertTrue(fs.mkdirs(path));
      assertEquals(++expectedLastInodeId, fsn.getLastInodeId());
      assertEquals(++inodeCount, fsn.dir.getInodeMapSize());
  
      Path filePath = new Path("/test1/file");
      fs.create(filePath);
      assertEquals(++expectedLastInodeId, fsn.getLastInodeId());
      assertEquals(++inodeCount, fsn.dir.getInodeMapSize());
  
      // Rename doesn't increase inode id
      Path renamedPath = new Path("/test2");
      fs.rename(path, renamedPath);
      fs.rename(renamedPath, path);
      assertEquals(expectedLastInodeId, fsn.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      
      // Delete a file and ensure inode map size decreases
      fs.delete(filePath, true);
      assertEquals(--inodeCount, fsn.dir.getInodeMapSize());
  
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNameNode().getNamesystem();
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
  
      // Make sure empty editlog can be handled
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNameNode().getNamesystem();
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private Path getInodePath(long inodeId, String remainingPath) {
    Path p = new Path(Path.SEPARATOR + FSDirectory.DOT_INODES_STRING
        + Path.SEPARATOR + inodeId + Path.SEPARATOR + remainingPath);
    System.out.println("Inode path is " + p);
    return p;
  }
  
  /**
   * Tests for addressing files using /.inodes/<inodeID> in file system
   * operations.
   */
  @Test
  public void testInodeIdBasedPaths() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_SUPPORT_FILEID_KEY, true);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(0, conf, 1, true, true, null, null);
      cluster.waitActive();
      DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
      NameNode nnRpc = cluster.getNameNode();
      
      // FileSystem#mkdirs "/testInodeIdBasedPaths"
      Path baseDir = getInodePath(INodeId.ROOT_INODE_ID, "testInodeIdBasedPaths");
      fs.mkdirs(baseDir);
      fs.exists(baseDir);
      long baseDirFileId = nnRpc.getExtendedFileInfo(baseDir.toString()).getFileId();
      
   
      // Test root inodeId Path
      Path rootPath = getInodePath(INodeId.ROOT_INODE_ID, "");
      long rootFileId = nnRpc.getExtendedFileInfo(rootPath.toString()).getFileId();
      assertEquals(rootFileId, INodeId.ROOT_INODE_ID);

      // FileSystem#create file and FileSystem#close
      Path testFileInodePath = getInodePath(baseDirFileId, "test1");
      Path testFileRegularPath = new Path(baseDir, "test1");
      final int testFileBlockSize = 1024;
      DFSTestUtil.createFile(fs, testFileInodePath, 1024, testFileBlockSize,
          1024, (short) 1, 0);
      assertTrue(fs.exists(testFileInodePath));
      
      // FileSystem#setPermission
      FsPermission perm = new FsPermission((short)0666);
      fs.setPermission(testFileInodePath, perm);
      
      // FileSystem#getFileStatus and FileSystem#getPermission
      HdfsFileStatus fileStatus = nnRpc.getFileInfo(testFileInodePath.toString());
      assertEquals(perm, fileStatus.getPermission());
      
      // FileSystem#setOwner
      fs.setOwner(testFileInodePath, fileStatus.getOwner(), fileStatus.getGroup());
      
      // FileSystem#setTimes
      fs.setTimes(testFileInodePath, 0, 0);
      fileStatus = nnRpc.getFileInfo(testFileInodePath.toString());
      assertEquals(0, fileStatus.getModificationTime());
      assertEquals(0, fileStatus.getAccessTime());
      
      // FileSystem#setReplication
      fs.setReplication(testFileInodePath, (short)3);
      fileStatus = nnRpc.getFileInfo(testFileInodePath.toString());
      assertEquals(3, fileStatus.getReplication());
      fs.setReplication(testFileInodePath, (short)1);
      
      // ClientProtocol#getPreferredBlockSize
      assertEquals(testFileBlockSize,
          nnRpc.getPreferredBlockSize(testFileInodePath.toString()));
      
      // FileSystem#append
//      fs.append(testFileInodePath);
      // DistributedFileSystem#recoverLease
      
      fs.recoverLease(testFileInodePath);
      // Namenode#getAdditionalBlock
      
      // TODO: NameNode#getAdditionalDatanode
      // FSNamesystem#abandonBlock
      // FSNamesystem#fsync
      
      // FileSystem#rename - both the variants
      Path renameDst = getInodePath(baseDirFileId, "test2");
      fileStatus = nnRpc.getFileInfo(testFileInodePath.toString());
      // Rename variant 1: rename and rename back
      fs.rename(testFileInodePath, renameDst);
      fs.rename(renameDst, testFileInodePath);
      checkEquals(fileStatus, nnRpc.getFileInfo(testFileInodePath.toString()));
      
      // FileSystem#getContentSummary
      assertEquals(fs.getContentSummary(testFileRegularPath).toString(),
          fs.getContentSummary(testFileInodePath).toString());
      
      // FileSystem#delete
      fs.delete(testFileInodePath, true);
      assertFalse(fs.exists(testFileInodePath));
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  private void checkEquals(HdfsFileStatus s1, HdfsFileStatus s2) {
    assertEquals(s1.getAccessTime(), s2.getAccessTime());
    assertEquals(s1.getBlockSize(), s2.getBlockSize());
    assertEquals(s1.getLocalName(), s2.getLocalName());
    assertEquals(s1.getOwner(), s2.getOwner());
    assertEquals(s1.getReplication(), s2.getReplication());
  }

  /**
   * If fileID feature is turned off, then the inode map should not be
   * initialized.
   */
  @Test
  public void testFileIDConfiguration() {
    Configuration conf = new Configuration();
    assertNull(FSDirectory.initInodeMap(conf, null));
  }
  
  /**
   * For a given path, build a tree of INodes and return the leaf node.
   */
  private INode getInodes(String path) {
    byte[][] components = INode.getPathComponents(path);
    FsPermission perm = FsPermission.createImmutable((short)0755);
    PermissionStatus permstatus = PermissionStatus.createImmutable("", "", perm);
    
    long id = 0;
    INodeDirectory prev = new INodeDirectory(++id, DFSUtil.string2Bytes(""),
        permstatus, 0);
    INodeDirectory dir = null;
    for (byte[] component : components) {
      if (component.length == 0) {
        continue;
      }
      System.out.println("Adding component " + DFSUtil.bytes2String(component));
      dir = new INodeDirectory(++id, component, permstatus, 0);
      prev.addChild(dir);
      prev = dir;
    }
    return dir; // Last Inode in the chain
  }
  
  private static void checkEquals(byte[][] expected, byte[][] actual) {
    assertEquals(expected.length, actual.length);
    int i = 0;
    for (byte[] e : expected) {
      assertTrue(Arrays.equals(e, actual[i++]));
    }
  }
  
  /**
   * Test for {@link FSDirectory#getPathComponents(INode)}
   */
  @Test
  public void testGetPathFromInode() {
    String path = "/a/b/c";
    INode inode = getInodes(path);
    byte[][] expected = INode.getPathComponents(path);
    byte[][] actual = FSDirectory.getPathComponents(inode);
    assertEquals(expected, actual);
  }
  
  /**
   * Tests for {@link FSDirectory#getPathComponents(String, FSDirectory)}
   * and for {@link FSDirectory#resolvePath(String, byte[][], FSDirectory)}
   */
  @Test
  public void testInodePath() throws FileNotFoundException {
    // For an non .inodes path the regular components are returned
    String path = "/a/b/c";
    byte[][] components = INode.getPathComponents(path);
    checkEquals(components, FSDirectory.getPathComponents(path, null));
    
    INode inode = getInodes(path);
    // For an any inode look up return inode corresponding to "c" from /a/b/c
    FSDirectory fsd = Mockito.mock(FSDirectory.class);
    Mockito.doReturn(inode).when(fsd).getInode(Mockito.anyLong());
    
    //
    // Tests for FSDirectory#getPathComponents()
    // Non inode regular path
    byte[][] result = FSDirectory.getPathComponents(path, fsd);
    checkEquals(components, result);
    
    // Inode path with no trailing separator
    result = FSDirectory.getPathComponents("/.inodes/1", fsd);
    checkEquals(components, result);
    
    // Inode path with trailing separator
    result = FSDirectory.getPathComponents("/.inodes/1/", fsd);
    checkEquals(components, result);
    
    // Inode relative path
    result = FSDirectory.getPathComponents("/.inodes/1/d/e/f", fsd);
    checkEquals(INode.getPathComponents("/a/b/c/d/e/f"), result);
    
    // A path with just .inodes 
    result = FSDirectory.getPathComponents("/.inodes", fsd);
    checkEquals(INode.getPathComponents("/.inodes"), result);
    
    //
    // Tests for FSDirectory#resolvePath()
    // Non inode regular path
    components = INode.getPathComponents(path);
    String resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals(path, resolvedPath);
    
    // Inode path with no trailing separator
    components = INode.getPathComponents("/.inodes/1");
    resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals(path, resolvedPath);
    
    // Inode path with trailing separator
    components = INode.getPathComponents("/.inodes/1/");
    resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals(path, resolvedPath);
    
    // Inode relative path
    components = INode.getPathComponents("/.inodes/1/d/e/f");
    resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals("/a/b/c/d/e/f", resolvedPath);
    
    // A path with just .inodes 
    components = INode.getPathComponents("/.inodes");
    resolvedPath = FSDirectory.resolvePath("/.inodes", components, fsd);
    assertEquals("/.inodes", resolvedPath);
  }
  
  /** File listing should should start with file @{code firstFile} */
  private void validateList(ExtendedDirectoryListing list, int firstFile,
      int expectedLength) {
    ExtendedHdfsFileStatus[] array = list.getPartialListing();
    assertEquals(expectedLength, array.length);
    int i = firstFile;
    for (ExtendedHdfsFileStatus fileStatus : array) {
      assertEquals(String.valueOf(i++), fileStatus.getLocalName());
    }
  }
  
  @Test
  public void testExtendedListing() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
    conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_SUPPORT_FILEID_KEY, true);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(0, conf, 1, true, true, null, null);
      cluster.waitActive();
      DistributedFileSystem fs = (DistributedFileSystem) cluster.getFileSystem();
      DFSClient client = new DFSClient(cluster.getNameNode()
          .getNameNodeAddress(), conf);
      Path baseDir = new Path("/testExtendedListing");
      fs.mkdirs(baseDir);
      for (int i = 0; i < 10; i++) { // Create 10 directories
        fs.mkdirs(new Path(baseDir, String.valueOf(i)));
      }
      
      // Test extended directory listing gives all the 10 directories
      ExtendedDirectoryListing list = client.listExtendedPaths(
          "/testExtendedListing", new byte[0]);
      validateList(list, 0, 10);
      
      // Test offset using a regular file name starting after directory "4"
      list = client.listExtendedPaths("/testExtendedListing",
          DFSUtil.string2Bytes("4"));
      validateList(list, 5, 5);
      
      // Test offset using a regular file name starting after directory "4"
      long fileIdForDir5 = list.getPartialListing()[0].getFileId();
      list = client.listExtendedPaths("/testExtendedListing",
          DFSUtil.string2Bytes(getInodePath(fileIdForDir5, "").toString()));
      validateList(list, 6, 4);
      
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
    
  }
}
