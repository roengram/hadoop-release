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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.junit.Test;
import org.mockito.Mockito;

public class TestINodeFile {
  public static final Log LOG = LogFactory.getLog(TestINodeFile.class);

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
      
      // Ensure right inode ID is returned in file status
      ExtendedHdfsFileStatus fileStatus = fsn.getExtendedFileInfo("/test1/file");
      assertEquals(expectedLastInodeId, fileStatus.getFileId());

      // Rename a directory
      // Last inode ID and inode map size should not change
      Path renamedPath = new Path("/test2");
      assertTrue(fs.rename(path, renamedPath));
      assertEquals(expectedLastInodeId, fsn.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      
      // Delete test2/file and test2 and ensure inode map size decreases
      assertTrue(fs.delete(renamedPath, true));
      inodeCount -= 2;
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());

      // Make sure fsimage can be loaded
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNameNode().getNamesystem();
      assertEquals(expectedLastInodeId, fsn.getLastInodeId());
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
  
      // Make sure empty editlog can be handled
      cluster.restartNameNode();
      cluster.waitActive();
      fsn = cluster.getNameNode().getNamesystem();
      assertEquals(inodeCount, fsn.dir.getInodeMapSize());
      assertEquals(expectedLastInodeId, fsn.getLastInodeId());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private Path getInodePath(long inodeId, String remainingPath) {
    StringBuilder b = new StringBuilder();
    b.append(Path.SEPARATOR).append(FSDirectory.DOT_RESERVED_STRING)
        .append(Path.SEPARATOR).append(FSDirectory.DOT_INODES_STRING)
        .append(Path.SEPARATOR).append(inodeId).append(Path.SEPARATOR)
        .append(remainingPath);
    Path p = new Path(b.toString());
    LOG.info("Inode path is " + p);
    return p;
  }
  
  /**
   * Tests for addressing files using /.reserved/.inodes/<inodeID> in file system
   * operations.
   */
  @Test
  public void testInodeIdBasedPaths() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
        DFSConfigKeys.DFS_BYTES_PER_CHECKSUM_DEFAULT);
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
      assertEquals(INodeId.ROOT_INODE_ID, rootFileId);

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
      
      // DistributedFileSystem#recoverLease
      fs.recoverLease(testFileInodePath);

      
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

  private void checkEquals(ExtendedHdfsFileStatus s1, ExtendedHdfsFileStatus s2) {
    checkEquals((HdfsFileStatus)s1, (HdfsFileStatus)s2);
    assertEquals(s1.getFileId(), s2.getFileId());
  }
  
  @Test
  public void testReplaceINode() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(0, conf, 1, true, true, null, null);
      cluster.waitActive();
      DistributedFileSystem fs = (DistributedFileSystem) cluster
          .getFileSystem();
      DFSClient client = new DFSClient(cluster.getNameNode()
          .getNameNodeAddress(), conf);
      Path file1 = new Path("/file1");
      FSDataOutputStream fso = fs.create(file1);
      
      // getExtendedFileInfo works for both regular name and inode id based name
      ExtendedHdfsFileStatus status = client.getExtendedFileInfo(file1.toString());
      String fileName = getInodePath(status.getFileId(), "").toString();
      ExtendedHdfsFileStatus status2 = client.getExtendedFileInfo(fileName);
      checkEquals(status, status2);

      // Close stream will trigger inode replacement from underConstruction to
      // complete inode. Sleep 1 second to make sure replacement is done.
      fso.close();
      Thread.sleep(1000);
      ExtendedHdfsFileStatus status3 = client.getExtendedFileInfo(fileName);
      checkEquals(status, status3);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * Check /.reserved path is reserved and cannot be created.
   */
  @Test
  public void testReservedFileNames() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      // First start a cluster with reserved file names check turned off
      cluster = new MiniDFSCluster(0, conf, 1, true, true, null, null);
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      
      // Creation of directory or file with reserved path names is disallowed
      ensureReservedFileNamesCannotBeCreated(fs, "/.reserved", false);
      ensureReservedFileNamesCannotBeCreated(fs, "/.reserved", false);
      Path reservedPath = new Path("/.reserved");
      
      // Loading of fsimage or editlog with /.reserved directory should fail
      // Mkdir "/.reserved reserved path with reserved path check turned off
      FSDirectory.CHECK_RESERVED_FILE_NAMES = false;
      fs.mkdirs(reservedPath);
      assertTrue(fs.isDirectory(reservedPath));
      ensureReservedFileNamesCannotBeLoaded(cluster);

      // Loading of fsimage or editlog with /.reserved file should fail
      // Create file "/.reserved reserved path with reserved path check turned off
      FSDirectory.CHECK_RESERVED_FILE_NAMES = false;
      ensureClusterRestartSucceeds(cluster);
      fs.delete(reservedPath, true);
      DFSTestUtil.createFile(fs, reservedPath, 10, (short)1, 0L);
      assertTrue(!fs.isDirectory(reservedPath));
      ensureReservedFileNamesCannotBeLoaded(cluster);
    } finally {
      FSDirectory.CHECK_RESERVED_FILE_NAMES = true;
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
  
  private void ensureReservedFileNamesCannotBeCreated(FileSystem fs, String name,
      boolean isDir) {
    // Creation of directory or file with reserved path names is disallowed
    Path reservedPath = new Path(name);
    try {
      if (isDir) {
        fs.mkdirs(reservedPath);
      } else {
        DFSTestUtil.createFile(fs, reservedPath, 10, (short) 1, 0L);
      }
      fail((isDir ? "mkdir" : "create file") + " should be disallowed");
    } catch (Exception expected) {
      // ignored
    }
  }
  
  private void ensureReservedFileNamesCannotBeLoaded(MiniDFSCluster cluster)
      throws IOException {
    // Turn on reserved file name checking. Loading of edits should fail
    FSDirectory.CHECK_RESERVED_FILE_NAMES = true;
    ensureClusterRestartFails(cluster);

    // Turn off reserved file name checking and successfully load edits
    FSDirectory.CHECK_RESERVED_FILE_NAMES = false;
    ensureClusterRestartSucceeds(cluster);

    // Turn on reserved file name checking. Loading of fsimage should fail
    FSDirectory.CHECK_RESERVED_FILE_NAMES = true;
    ensureClusterRestartFails(cluster);
  }
  
  private void ensureClusterRestartFails(MiniDFSCluster cluster) {
    try {
      cluster.restartNameNode();
      fail("Cluster should not have successfully started");
    } catch (Exception expected) {
      LOG.info("Expected exception thrown " + expected);
    }
    assertFalse(cluster.isClusterUp());
  }
  
  private void ensureClusterRestartSucceeds(MiniDFSCluster cluster)
      throws IOException {
    cluster.restartNameNode();
    cluster.waitActive();
    assertTrue(cluster.isClusterUp());
  }
  
  /**
   * For a given path, build a tree of INodes and return the leaf node.
   */
  private INode createTreeOfInodes(String path) throws QuotaExceededException {
    byte[][] components = INode.getPathComponents(path);
    FsPermission perm = FsPermission.createImmutable((short)0755);
    PermissionStatus permstatus = PermissionStatus.createImmutable("", "", perm);
    
    long id = 0;
    INodeDirectory prev = new INodeDirectory(++id, new byte[0], permstatus, 0);
    INodeDirectory dir = null;
    for (byte[] component : components) {
      if (component.length == 0) {
        continue;
      }
      dir = new INodeDirectory(++id, component, permstatus, 0);
      prev.addChild(dir, false, null);
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
  public void testGetPathFromInode() throws QuotaExceededException {
    String path = "/a/b/c";
    INode inode = createTreeOfInodes(path);
    byte[][] expected = INode.getPathComponents(path);
    byte[][] actual = FSDirectory.getPathComponents(inode);
    checkEquals(expected, actual);
  }
  
  /**
   * Tests for {@link FSDirectory#resolvePath(String, byte[][], FSDirectory)}
   */
  @Test
  public void testInodePath() throws IOException {
    // For a non .inodes path the regular components are returned
    String path = "/a/b/c";
    INode inode = createTreeOfInodes(path);
    // For an any inode look up return inode corresponding to "c" from /a/b/c
    FSDirectory fsd = Mockito.mock(FSDirectory.class);
    FsPermission perm = FsPermission.createImmutable((short)0755);
    PermissionStatus permstatus = PermissionStatus.createImmutable("", "", perm);
    INode rootInode = new INodeDirectory(INodeId.ROOT_INODE_ID, INodeDirectory.ROOT_NAME, permstatus, 0);
    Mockito.when(fsd.getInode(Mockito.anyLong())).thenReturn(inode);
    //Mockito.doReturn(inode).when(fsd).getInode(
    
    // Null components
    assertEquals("/test", FSDirectory.resolvePath("/test", null, fsd));
    
    // Tests for FSDirectory#resolvePath()
    // Non inode regular path
    byte[][] components = INode.getPathComponents(path);
    String resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals(path, resolvedPath);
    
    // Inode path with no trailing separator
    components = INode.getPathComponents("/.reserved/.inodes/1");
    resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals(path, resolvedPath);
    
    // Inode path with trailing separator
    components = INode.getPathComponents("/.reserved/.inodes/1/");
    assertEquals(path, resolvedPath);
    
    // Inode relative path
    components = INode.getPathComponents("/.reserved/.inodes/1/d/e/f");
    resolvedPath = FSDirectory.resolvePath(path, components, fsd);
    assertEquals("/a/b/c/d/e/f", resolvedPath);
    
    // A path with just .inodes  returns the path as is
    String testPath = "/.reserved/.inodes";
    components = INode.getPathComponents(testPath);
    resolvedPath = FSDirectory.resolvePath(testPath, components, fsd);
    assertEquals(testPath, resolvedPath);
    
    // An invalid inode path should remain unresolved
    testPath = "/.invalid/.inodes/1";
    components = INode.getPathComponents(testPath);
    resolvedPath = FSDirectory.resolvePath(testPath, components, fsd);
    assertEquals(testPath, resolvedPath);
    
    // Root inode path
    Mockito.when(fsd.getInode(INodeId.ROOT_INODE_ID)).thenReturn(rootInode);
    testPath = "/.reserved/.inodes/" + INodeId.ROOT_INODE_ID;
    components = INode.getPathComponents(testPath);
    resolvedPath = FSDirectory.resolvePath(testPath, components, fsd);
    assertEquals("/", resolvedPath);
  }
}
