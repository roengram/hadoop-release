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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.SnapshotTestHelper;
import org.apache.hadoop.hdfs.server.namenode.SnapshotTestHelper.TestDirectoryTree;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests snapshot functionality. One or multiple snapshots are
 * created. The snapshotted directory is changed and verification is done to
 * ensure snapshots remain unchanges.
 */
public class TestSnapshot {
  {
    ((Log4JLogger) INode.LOG).getLogger().setLevel(Level.ALL);
    SnapshotTestHelper.disableLogs();
  }
  
  private static final long seed = System.currentTimeMillis();
  protected static final short REPLICATION = 3;
  protected static final int BLOCKSIZE = 1024;
  /** The number of times snapshots are created for a snapshottable directory  */
  public static final int SNAPSHOT_ITERATION_NUMBER = 20;
  /** Height of directory tree used for testing */
  public static final int DIRECTORY_TREE_LEVEL = 5;
  
  protected Configuration conf;
  protected MiniDFSCluster cluster;
  protected static FSNamesystem fsn;
  protected static FSDirectory fsdir;
  protected DistributedFileSystem hdfs;

  private static Random random = new Random(seed);
  
  /**
   * The list recording all previous snapshots. Each element in the array
   * records a snapshot root.
   */
  protected static ArrayList<Path> snapshotList = new ArrayList<Path>();
  /**
   * Check {@link SnapshotTestHelper.TestDirectoryTree}
   */
  private TestDirectoryTree dirTree;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    cluster = new MiniDFSCluster(conf, REPLICATION, true, null);
    cluster.waitActive();

    fsn = cluster.getNameNode().getNamesystem();
    fsdir = fsn.dir;
    hdfs = (DistributedFileSystem) cluster.getFileSystem();
    dirTree = new TestDirectoryTree(DIRECTORY_TREE_LEVEL, hdfs);
  }

  @After
  public void tearDown() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  static int modificationCount = 0;
  /**
   * Make changes (modification, deletion, creation) to the current files/dir.
   * Then check if the previous snapshots are still correct.
   * 
   * @param modifications Modifications that to be applied to the current dir.
   */
  private void modifyCurrentDirAndCheckSnapshots(Modification[] modifications)
      throws Exception {
    for (Modification modification : modifications) {
      System.out.println(++modificationCount + ") " + modification);
      modification.loadSnapshots();
      modification.modify();
      modification.checkSnapshots();
    }
  }

  /**
   * Create two snapshots in each iteration. Each time we will create a snapshot
   * for the top node, then randomly pick a dir in the tree and create
   * snapshot for it.
   * 
   * Finally check the snapshots are created correctly.
   */
  protected TestDirectoryTree.Node[] createSnapshots() throws Exception {
    TestDirectoryTree.Node[] nodes = new TestDirectoryTree.Node[2];
    // Each time we will create a snapshot for the top level dir
    Path root = SnapshotTestHelper.createSnapshot(hdfs,
        dirTree.topNode.nodePath, nextSnapshotName());
    snapshotList.add(root);
    nodes[0] = dirTree.topNode; 
    SnapshotTestHelper.checkSnapshotCreation(hdfs, root, nodes[0].nodePath);
    
    // Then randomly pick one dir from the tree (cannot be the top node) and
    // create snapshot for it
    ArrayList<TestDirectoryTree.Node> excludedList = 
        new ArrayList<TestDirectoryTree.Node>();
    excludedList.add(nodes[0]);
    nodes[1] = dirTree.getRandomDirNode(random, excludedList);

    root = SnapshotTestHelper.createSnapshot(hdfs, nodes[1].nodePath,
        nextSnapshotName());

    snapshotList.add(root);
    SnapshotTestHelper.checkSnapshotCreation(hdfs, root, nodes[1].nodePath);
    return nodes;
  }

  
  /**
   * Main test, where we will go in the following loop:
   * <pre>
   *    Create snapshot and check the creation <--+  
   * -> Change the current/live files/dir         | 
   * -> Check previous snapshots -----------------+
   * </pre>
   */
  @Test
  public void testSnapshot() throws Throwable {
    try {
      runTestSnapshot();
    } catch(Throwable t) {
      SnapshotTestHelper.LOG.info("FAILED", t);
      SnapshotTestHelper.dumpTreeRecursively(fsdir.getINode("/"));
      throw t;
    }
  }

  private void runTestSnapshot() throws Exception {
    for (int i = 0; i < SNAPSHOT_ITERATION_NUMBER; i++) {
      // create snapshot and check the creation
      TestDirectoryTree.Node[] ssNodes = createSnapshots();
      
      // prepare the modifications for the snapshotted dirs
      // we cover the following directories: top, new, and a random
      ArrayList<TestDirectoryTree.Node> excludedList = 
          new ArrayList<TestDirectoryTree.Node>();
      TestDirectoryTree.Node[] modNodes = 
          new TestDirectoryTree.Node[ssNodes.length + 1];
      for (int n = 0; n < ssNodes.length; n++) {
        modNodes[n] = ssNodes[n];
        excludedList.add(ssNodes[n]);
      }
      modNodes[modNodes.length - 1] = dirTree.getRandomDirNode(random,
          excludedList);
      Modification[] mods = prepareModifications(modNodes);
      // make changes to the directories/files
      modifyCurrentDirAndCheckSnapshots(mods);
      
      // also update the metadata of directories
      TestDirectoryTree.Node chmodDir = dirTree.getRandomDirNode(random, null);
      Modification chmod = new FileChangePermission(chmodDir.nodePath, hdfs,
          genRandomPermission());
      String[] userGroup = genRandomOwner();
      TestDirectoryTree.Node chownDir = dirTree.getRandomDirNode(random,
          Arrays.asList(chmodDir));
      Modification chown = new FileChown(chownDir.nodePath, hdfs, userGroup[0],
          userGroup[1]);
      modifyCurrentDirAndCheckSnapshots(new Modification[]{chmod, chown});
    }
    System.out.println("XXX done:");
    SnapshotTestHelper.dumpTreeRecursively(fsn.getFSDirectory().getINode("/"));
  }
  
  /**
   * Creating snapshots for a directory that is not snapshottable must fail.
   * 
   * TODO: Listing/Deleting snapshots for a directory that is not snapshottable
   * should also fail.
   */
  @Test
  public void testSnapshottableDirectory() throws Exception {
    Path dir = new Path("/TestSnapshot/sub");
    Path file0 = new Path(dir, "file0");
    Path file1 = new Path(dir, "file1");
    DFSTestUtil.createFile(hdfs, file0, BLOCKSIZE, REPLICATION, seed);
    DFSTestUtil.createFile(hdfs, file1, BLOCKSIZE, REPLICATION, seed);

    try {
      hdfs.createSnapshot(dir, "s1");
      fail("Did not throw IOException when creating snapshots for a non-snapshottable directory");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains(
          "Directory is not a snapshottable directory: " + dir.toString()));
    }
  }

  /**
   * Prepare a list of modifications. A modification may be a file creation,
   * file deletion, or a modification operation such as appending to an existing
   * file.
   */
  private Modification[] prepareModifications(TestDirectoryTree.Node[] nodes)
      throws Exception {
    ArrayList<Modification> mList = new ArrayList<Modification>();
    for (TestDirectoryTree.Node node : nodes) {
      // If the node does not have files in it, create files
      if (node.fileList == null) {
        node.initFileList(node.nodePath.getName(), BLOCKSIZE, REPLICATION, seed, 5);
      }
      
      //
      // Modification iterations are as follows:
      // Iteration 0 - create:fileList[4], delete:fileList[0],
      //               chmod:fileList[1],  chown:fileList[2],  
      //               change_replication:fileList[3].
      //               Set nullFileIndex to 0
      //
      // Iteration 1 - create:fileList[0], delete:fileList[1],
      //               chmod:fileList[2],  chown:fileList[3],  
      //               change_replication:fileList[4]
      //               Set nullFileIndex to 1
      // 
      // Iteration 2 - create:fileList[1], delete:fileList[2],
      //               chmod:fileList[3],  chown:fileList[4],  
      //               change_replication:fileList[0]
      //               Set nullFileIndex to 2
      // ...
      //
      Modification create = new FileCreation(
          node.fileList.get(node.nullFileIndex), hdfs, (int) BLOCKSIZE);
      Modification delete = new FileDeletion(
          node.fileList.get((node.nullFileIndex + 1) % node.fileList.size()),
          hdfs);
      Modification chmod = new FileChangePermission(
          node.fileList.get((node.nullFileIndex + 2) % node.fileList.size()),
          hdfs, genRandomPermission());
      String[] userGroup = genRandomOwner();
      Modification chown = new FileChown(
          node.fileList.get((node.nullFileIndex + 3) % node.fileList.size()),
          hdfs, userGroup[0], userGroup[1]);
      Modification replication = new FileChangeReplication(
          node.fileList.get((node.nullFileIndex + 4) % node.fileList.size()),
          hdfs, (short) (random.nextInt(REPLICATION) + 1));
      node.nullFileIndex = (node.nullFileIndex + 1) % node.fileList.size();
      Modification dirChange = new DirCreationOrDeletion(node.nodePath, hdfs,
          node, random.nextBoolean());
      
      mList.add(create);
      mList.add(delete);
      mList.add(chmod);
      mList.add(chown);
      mList.add(replication);
      mList.add(dirChange);
    }
    return mList.toArray(new Modification[0]);
  }
  
  /**
   * @return A random FsPermission
   */
  private FsPermission genRandomPermission() {
    // randomly select between "rwx" and "rw-"
    FsAction u = random.nextBoolean() ? FsAction.ALL : FsAction.READ_WRITE;
    FsAction g = random.nextBoolean() ? FsAction.ALL : FsAction.READ_WRITE;
    FsAction o = random.nextBoolean() ? FsAction.ALL : FsAction.READ_WRITE;
    return new FsPermission(u, g, o);
  }
  
  /**
   * @return A string array containing two string: the first string indicates
   *         the owner, and the other indicates the group
   */
  private String[] genRandomOwner() {
    // TODO
    String[] userGroup = new String[]{"dr.who", "unknown"};
    return userGroup;
  }
  
  
  private static int snapshotCount = 0;

  /** @return The next snapshot name */
  static String nextSnapshotName() {
    return String.format("s-%d", ++snapshotCount);
  }

  /**
   * Base class to present changes applied to current file/dir. A modification
   * can be file creation, deletion, or other modifications such as appending on
   * an existing file. Three abstract methods need to be implemented by
   * subclasses: loadSnapshots() captures the states of snapshots before the
   * modification, modify() applies the modification to the current directory,
   * and checkSnapshots() verifies the snapshots do not change after the
   * modification.
   */
  static abstract class Modification {
    protected final Path file;
    protected final FileSystem fs;
    final String type;

    Modification(Path file, FileSystem fs, String type) {
      this.file = file;
      this.fs = fs;
      this.type = type;
    }

    abstract void loadSnapshots() throws Exception;

    abstract void modify() throws Exception;

    abstract void checkSnapshots() throws Exception;
    
    @Override
    public String toString() {
      return getClass().getSimpleName() + ":" + type + ":" + file;
    }
  }

  /**
   * Modifications that change the file status. We check the FileStatus of
   * snapshot files before/after the modification.
   */
  static abstract class FileStatusChange extends Modification {
    protected final HashMap<Path, FileStatus> statusMap;
    
    FileStatusChange(Path file, FileSystem fs, String type) {
      super(file, fs, type);
      statusMap = new HashMap<Path, FileStatus>();
    }
     
    @Override
    void loadSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = SnapshotTestHelper.getSnapshotFile(fs,
            snapshotRoot, file);
        if (snapshotFile != null) {
          if (fs.exists(snapshotFile)) {
            FileStatus status = fs.getFileStatus(snapshotFile);
            statusMap.put(snapshotFile, status);
          } else {
            statusMap.put(snapshotFile, null);
          }
        }
      }
    }
    
    @Override
    void checkSnapshots() throws Exception {
      for (Path snapshotFile : statusMap.keySet()) {
        FileStatus currentStatus = fs.exists(snapshotFile) ? fs
            .getFileStatus(snapshotFile) : null;
        FileStatus originalStatus = statusMap.get(snapshotFile);
        assertEquals(currentStatus, originalStatus);
        if (currentStatus != null) {
          String s = null;
          if (!currentStatus.toString().equals(originalStatus.toString())) {
            s = "FAILED: " + getClass().getSimpleName()
                + ": file="  + file + ", snapshotFile" + snapshotFile
                + "\n\n currentStatus = " + currentStatus
                +   "\noriginalStatus = " + originalStatus
                + "\n\nfile        : " + fsdir.getINode(file.toString()).toDetailString()
                + "\n\nsnapshotFile: " + fsdir.getINode(snapshotFile.toString()).toDetailString();
            
            System.out.println(s);
            SnapshotTestHelper.dumpTreeRecursively(fsdir.getINode("/"));
          }
          assertEquals(s, currentStatus.toString(), originalStatus.toString());
        }
      }
    }
  }
  
  /**
   * Change the file permission
   */
  static class FileChangePermission extends FileStatusChange {
    private final FsPermission newPermission;
    
    FileChangePermission(Path file, FileSystem fs, FsPermission newPermission) {
      super(file, fs, "chmod");
      this.newPermission = newPermission;
    }
        
    @Override
    void modify() throws Exception {
      assertTrue(fs.exists(file));
      fs.setPermission(file, newPermission);
    }
  }
  
  /**
   * Change the replication factor of file
   */
  static class FileChangeReplication extends FileStatusChange {
    private final short newReplication;
    
    FileChangeReplication(Path file, FileSystem fs, short replication) {
      super(file, fs, "replication");
      this.newReplication = replication;
    }
    
    @Override
    void modify() throws Exception {
      assertTrue(fs.exists(file));
      fs.setReplication(file, newReplication);
    }
  }
  
  /**
   * Change the owner:group of a file
   */
  static class FileChown extends FileStatusChange {
    private final String newUser;
    private final String newGroup;
    
    FileChown(Path file, FileSystem fs, String user, String group) {
      super(file, fs, "chown");
      this.newUser = user;
      this.newGroup = group;
    }

    @Override
    void modify() throws Exception {
      assertTrue(fs.exists(file));
      fs.setOwner(file, newUser, newGroup);
    }
  }

  /**
   * New file creation
   */
  static class FileCreation extends Modification {
    final int fileLen;
    private final HashMap<Path, FileStatus> fileStatusMap;

    FileCreation(Path file, FileSystem fs, int len) {
      super(file, fs, "creation");
      assert len >= 0;
      this.fileLen = len;
      fileStatusMap = new HashMap<Path, FileStatus>();
    }

    @Override
    void loadSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = SnapshotTestHelper.getSnapshotFile(fs,
            snapshotRoot, file);
        if (snapshotFile != null) {
          FileStatus status = 
              fs.exists(snapshotFile) ? fs.getFileStatus(snapshotFile) : null; 
          fileStatusMap.put(snapshotFile, status);
        }
      }
    }

    @Override
    void modify() throws Exception {
      DFSTestUtil.createFile(fs, file, fileLen, REPLICATION, seed);
    }

    @Override
    void checkSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotFile = SnapshotTestHelper.getSnapshotFile(fs,
            snapshotRoot, file);
        if (snapshotFile != null) {
          boolean computed = fs.exists(snapshotFile);
          boolean expected = fileStatusMap.get(snapshotFile) != null;
          assertEquals(expected, computed);
          if (computed) {
            FileStatus currentSnapshotStatus = fs.getFileStatus(snapshotFile);
            FileStatus originalStatus = fileStatusMap.get(snapshotFile);
            // We compare the string because it contains all the information,
            // while FileStatus#equals only compares the path
            assertEquals(currentSnapshotStatus.toString(),
                originalStatus.toString());
          }
        }
      }
    }
  }
  
  /**
   * File deletion
   */
  static class FileDeletion extends Modification {
    private final HashMap<Path, Boolean> snapshotFileExistenceMap;

    FileDeletion(Path file, FileSystem fs) {
      super(file, fs, "deletion");
      snapshotFileExistenceMap = new HashMap<Path, Boolean>();
    }

    @Override
    void loadSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        boolean existence = SnapshotTestHelper.getSnapshotFile(fs,
            snapshotRoot, file) != null;
        snapshotFileExistenceMap.put(snapshotRoot, existence);
      }
    }

    @Override
    void modify() throws Exception {
      fs.delete(file, true);
    }

    @Override
    void checkSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        boolean currentSnapshotFileExist = SnapshotTestHelper.getSnapshotFile(
            fs, snapshotRoot, file) != null;
        boolean originalSnapshotFileExist = snapshotFileExistenceMap
            .get(snapshotRoot);
        assertEquals(currentSnapshotFileExist, originalSnapshotFileExist);
      }
    }
  }
  
  /**
   * Directory creation or deletion.
   */
  static class DirCreationOrDeletion extends Modification {
    private final TestDirectoryTree.Node node;
    private final boolean isCreation;
    private final Path changedPath;
    private final HashMap<Path, FileStatus> statusMap;
    
    DirCreationOrDeletion(Path file, FileSystem fs, TestDirectoryTree.Node node,
        boolean isCreation) {
      super(file, fs, "dircreation");
      this.node = node;
      // If the node's nonSnapshotChildren is empty, we still need to create
      // sub-directories
      this.isCreation = isCreation || node.nonSnapshotChildren.isEmpty();
      if (this.isCreation) {
        // Generate the path for the dir to be created
        changedPath = new Path(node.nodePath, "sub"
            + node.nonSnapshotChildren.size());
      } else {
        // If deletion, we delete the current last dir in nonSnapshotChildren
        changedPath = node.nonSnapshotChildren.get(node.nonSnapshotChildren
            .size() - 1).nodePath;
      }
      this.statusMap = new HashMap<Path, FileStatus>();
    }
    
    @Override
    void loadSnapshots() throws Exception {
      for (Path snapshotRoot : snapshotList) {
        Path snapshotDir = SnapshotTestHelper.getSnapshotFile(fs, snapshotRoot,
            changedPath);
        if (snapshotDir != null) {
          FileStatus status = fs.exists(snapshotDir) ? fs
              .getFileStatus(snapshotDir) : null;
          statusMap.put(snapshotDir, status);
          // In each non-snapshottable directory, we also create a file. Thus
          // here we also need to check the file's status before/after taking
          // snapshots
          Path snapshotFile = new Path(snapshotDir, "file0");
          status = fs.exists(snapshotFile) ? fs.getFileStatus(snapshotFile)
              : null;
          statusMap.put(snapshotFile, status);
        }
      }
    }

    @Override
    void modify() throws Exception {
      if (isCreation) {
        // creation
        TestDirectoryTree.Node newChild = new TestDirectoryTree.Node(
            changedPath, node.level + 1, node, node.fs);
        // create file under the new non-snapshottable directory
        newChild.initFileList(node.nodePath.getName(), BLOCKSIZE, REPLICATION, seed, 2);
        node.nonSnapshotChildren.add(newChild);
      } else {
        // deletion
        TestDirectoryTree.Node childToDelete = node.nonSnapshotChildren
            .remove(node.nonSnapshotChildren.size() - 1);
        node.fs.delete(childToDelete.nodePath, true);
      }
    }

    @Override
    void checkSnapshots() throws Exception {
      for (Path snapshot : statusMap.keySet()) {
        FileStatus currentStatus = fs.exists(snapshot) ? fs
            .getFileStatus(snapshot) : null;
        FileStatus originalStatus = statusMap.get(snapshot);
        assertEquals(currentStatus, originalStatus);
        if (currentStatus != null) {
          assertEquals(currentStatus.toString(), originalStatus.toString());
        }
      }
    } 
  } 
}