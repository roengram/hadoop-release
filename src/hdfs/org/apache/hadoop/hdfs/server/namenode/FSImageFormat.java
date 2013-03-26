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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.FSImage.DatanodeImage;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory.INodesInPath;
import org.apache.hadoop.hdfs.server.namenode.snapshot.FileWithSnapshot.FileDiffList;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileUnderConstructionWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

public class FSImageFormat {
  private static final Log LOG = FSImage.LOG;
  
  // Static-only class
  private FSImageFormat() {}
  
  /**
   * A one-shot class responsible for loading an image. The load() function
   * should be called once, after which the getter methods may be used to retrieve
   * information about the image that was loaded, if loading was successful.
   */
  public static class Loader {
    private final StorageInfo storage;
    private final FSNamesystem namesystem;
    private final FSDirectory fsDir;
    
    public Loader(StorageInfo storage) {
      this.storage = storage;
      namesystem = FSNamesystem.getFSNamesystem();
      fsDir = namesystem.dir;
    }
    
    /** @return The FSDirectory of the namesystem where the fsimage is loaded */
    public FSDirectory getFSDirectoryInLoading() {
      return namesystem.dir;
    }
    
    /**
     * Load in the filesystem imagefrom file. It's a big list of filenames and
     * blocks. Return whether we should "re-save" and consolidate the edit-logs.
     */
    boolean load(File curFile, boolean loadNamespaceId) throws IOException {
      assert storage.getLayoutVersion() < 0 : "Negative layout version is expected.";
      assert curFile != null : "curFile is null";

      //
      // Load in bits
      //
      boolean needToSave = true;
      DataInputStream in = new DataInputStream(new BufferedInputStream(
                                new FileInputStream(curFile)));
      try {
        /*
         * Note: Remove any checks for version earlier than 
         * Storage.LAST_UPGRADABLE_LAYOUT_VERSION since we should never get 
         * to here with older images.
         */
        
        /*
         * TODO we need to change format of the image file
         * it should not contain version and namespace fields
         */
        // read image version: first appeared in version -1
        int imgVersion = in.readInt();
        // read namespaceID: first appeared in version -2
        int namespaceId = in.readInt();
        if (loadNamespaceId) {
          storage.namespaceID = namespaceId;
        }

        // read number of files
        long numFiles;
        if (imgVersion <= -16) {
          numFiles = in.readLong();
        } else {
          numFiles = in.readInt();
        }

        storage.layoutVersion = imgVersion;
        // read in the last generation stamp.
        if (imgVersion <= -12) {
          long genstamp = in.readLong();
          namesystem.setGenerationStamp(genstamp); 
        }

        needToSave = (imgVersion != FSConstants.LAYOUT_VERSION);

        if (storage.layoutVersion <= -42) {
          namesystem.getSnapshotManager().read(in);
          loadLocalNameINodesWithSnapshot(in);
        } else {
          loadFullNameINodes(numFiles, in);
        }
        
        // load datanode info
        this.loadDatanodes(imgVersion, in);

        // load Files Under Construction
        this.loadFilesUnderConstruction(imgVersion, in);
        
        this.loadSecretManagerState(imgVersion, in, namesystem);
        
      } finally {
        in.close();
      }
      
      return needToSave;
    }
    
    /**
     * Load fsimage files when 1) only local names are stored, 
     * and 2) snapshot is supported.
     * 
     * @param in Image input stream
     */
    private void loadLocalNameINodesWithSnapshot(DataInputStream in)
        throws IOException {
      // load root
      loadRoot(in);
      // load rest of the nodes recursively
      loadDirectoryWithSnapshot(in);
    }
    
    /**
     * Load a directory when snapshot is supported.
     * @param in The {@link DataInputStream} instance to read.
     */
    private void loadDirectoryWithSnapshot(DataInputStream in)
        throws IOException {
      // Step 1. Identify the parent INode
      String parentPath = FSImageSerialization.readString(in);
      final INodeDirectory parent = INodeDirectory.valueOf(
          namesystem.dir.rootDir.getNode(parentPath), parentPath);
      
      // Step 2. Load snapshots if parent is snapshottable
      int numSnapshots = in.readInt();
      if (numSnapshots >= 0) {
        final INodeDirectorySnapshottable snapshottableParent
            = INodeDirectorySnapshottable.valueOf(parent, parentPath);
        if (snapshottableParent.getParent() != null) { // not root
          this.namesystem.getSnapshotManager().addSnapshottable(
              snapshottableParent);
        }
        // load snapshots and snapshotQuota
        SnapshotFSImageFormat.loadSnapshotList(snapshottableParent,
            numSnapshots, in, this);
      }

      // Step 3. Load children nodes under parent
      loadChildren(parent, in);
      
      // Step 4. load Directory Diff List
      SnapshotFSImageFormat.loadDirectoryDiffList(parent, in, this);
      
      // Recursively load sub-directories, including snapshot copies of deleted
      // directories
      int numSubTree = in.readInt();
      for (int i = 0; i < numSubTree; i++) {
        loadDirectoryWithSnapshot(in);
      }
    }
    
    /** Load children nodes for the parent directory. */
    private int loadChildren(INodeDirectory parent, DataInputStream in)
        throws IOException {
      int numChildren = in.readInt();
      for (int i = 0; i < numChildren; i++) {
        // load single inode
        INode newNode = loadINodeWithLocalName(false, in);
        addToParent(parent, newNode);
      }
      return numChildren;
    }
    
    public INode loadINodeWithLocalName(boolean isSnapshotINode,
        DataInputStream in) throws IOException {
      final byte[] localName = new byte[in.readShort()];
      in.readFully(localName);
      final INode inode = loadINode(localName, isSnapshotINode, in);
      inode.setLocalName(localName);
      return inode;
    }
    
    /**
     * Load information about root, and use the information to update the root
     * directory of NameSystem.
     * @param in The {@link DataInputStream} instance to read.
     */
    private void loadRoot(DataInputStream in) throws IOException {
      // load root
      if (in.readShort() != 0) {
        throw new IOException("First node is not root");
      }
      final INode root = loadINode(null, false, in);
      // update the root's attributes
      updateRootAttr(root);
    }
    
    private void loadFullNameINodes(long numFiles, DataInputStream in)
        throws IOException {
      LOG.info("Number of files = " + numFiles);
      byte[][] pathComponents;
      byte[][] parentPath = {{}};
      INodeDirectory parentINode = fsDir.rootDir;
      
      for (long i = 0; i < numFiles; i++) {
        pathComponents = FSImageSerialization.readPathComponents(in);
        final INode newNode = loadINode(pathComponents[pathComponents.length-1],
            false, in);
        
        if (isRoot(pathComponents)) { // it is the root
          // update the root's attributes
          updateRootAttr(newNode);
          continue;
        }
        // check if the new inode belongs to the same parent
        if(!isParent(pathComponents, parentPath)) {
          parentINode = getParentINodeDirectory(pathComponents);
          parentPath = getParent(pathComponents);
        }
        
        // add new inode
        addToParent(parentINode, newNode);
      }
    }
    
    /**
     * Add the child node to parent and, if child is a file, update block map.
     * This method is only used for image loading so that synchronization,
     * modification time update and space count update are not needed.
     */
    private void addToParent(INodeDirectory parent, INode child) {
      // NOTE: This does not update space counts for parents
      if (!parent.addChild(child)) {
        return;
      }
      namesystem.dir.cacheName(child);

      if (child.isFile()) {
        // Add file->block mapping
        final INodeFile file = child.asFile();
        final BlockInfo[] blocks = file.getBlocks();
        if (blocks != null) {
          for (int i = 0; i < blocks.length; i++) {
            file.setBlock(i, namesystem.blocksMap.addINode(blocks[i], file));
          } 
        }
      }
    }
    
    private boolean isRoot(byte[][] path) {
      return path.length == 1 &&
        path[0] == null;    
    }
    
    /** Update the root node's attributes */
    private void updateRootAttr(INode root) {                                                           
      long nsQuota = root.getNsQuota();
      long dsQuota = root.getDsQuota();
      if (nsQuota != -1 || dsQuota != -1) {
        fsDir.rootDir.setQuota(nsQuota, dsQuota);
      }
      fsDir.rootDir.cloneModificationTime(root);
      fsDir.rootDir.clonePermissionStatus(root);    
    }
    
    private INodeDirectory getParentINodeDirectory(byte[][] pathComponents)
        throws FileNotFoundException {
      if (pathComponents.length < 2) { // root
        return null;
      }
      // Get the parent INode
      final INodesInPath inodes = namesystem.dir.getExistingPathINodes(
          pathComponents);
      return INodeDirectory.valueOf(inodes.getINode(-2), pathComponents);
    }
    
    byte[][] getParent(byte[][] path) {
      byte[][] result = new byte[path.length - 1][];
      for (int i = 0; i < result.length; i++) {
        result[i] = new byte[path[i].length];
        System.arraycopy(path[i], 0, result[i], 0, path[i].length);
      }
      return result;
    }

    private boolean isParent(byte[][] path, byte[][] parent) {
      if (path == null || parent == null)
        return false;
      if (parent.length == 0 || path.length != parent.length + 1)
        return false;
      boolean isParent = true;
      for (int i = 0; i < parent.length; i++) {
        isParent = isParent && Arrays.equals(path[i], parent[i]); 
      }
      return isParent;
    }
    
    private INode loadINode(final byte[] localName, boolean isSnapshotINode,
        DataInputStream in) throws IOException {
      int layoutVersion = storage.getLayoutVersion();
      short replication = FSEditLog.adjustReplication(in.readShort());
      long modificationTime = in.readLong();
      long atime = 0;
      
      if (layoutVersion <= -17) {
        atime = in.readLong();
      }
      long blockSize = 0;
      if (layoutVersion <= -8) {
        blockSize = in.readLong();
      }
      int numBlocks = in.readInt();
      BlockInfo blocks[] = null;

      // for older versions, a blocklist of size 0
      // indicates a directory.
      if ((-9 <= layoutVersion && numBlocks > 0) ||
          (layoutVersion < -9 && numBlocks >= 0)) {
        blocks = new BlockInfo[numBlocks];
        for (int j = 0; j < numBlocks; j++) {
          blocks[j] = new BlockInfo(replication);
          if (-14 < layoutVersion) {
            blocks[j].set(in.readLong(), in.readLong(), 
                          Block.GRANDFATHER_GENERATION_STAMP);
          } else {
            blocks[j].readFields(in);
          }
        }
        
        // Older versions of HDFS does not store the block size in inode.
        // If the file has more than one block, use the size of the 
        // first block as the blocksize. Otherwise use the default block size.
        //
        if (-8 <= layoutVersion && blockSize == 0) {
          if (numBlocks > 1) {
            blockSize = blocks[0].getNumBytes();
          } else {
            long first = ((numBlocks == 1) ? blocks[0].getNumBytes(): 0);
            blockSize = Math.max(namesystem.getDefaultBlockSize(), first);
          }
        }
        
        String clientName = "";
        String clientMachine = "";
        boolean underConstruction = false;
        FileDiffList fileDiffs = null;
        if (layoutVersion <= -42) { // support snapshot
          // read diffs
          fileDiffs = SnapshotFSImageFormat.loadFileDiffList(in, this);

          if (isSnapshotINode) {
            underConstruction = in.readBoolean();
            if (underConstruction) {
              clientName = FSImageSerialization.readString(in);
              clientMachine = FSImageSerialization.readString(in);
            }
          }
        }

        PermissionStatus permissions = namesystem.getUpgradePermission();
        if (layoutVersion <= -11) {
          permissions = PermissionStatus.read(in);
        }

        // return
        final INodeFile file = new INodeFile(localName, permissions,
            modificationTime, atime, blocks, replication, blockSize);
        return fileDiffs != null? new INodeFileWithSnapshot(file, fileDiffs)
            : underConstruction? new INodeFileUnderConstruction(
                file, clientName, clientMachine, null)
            : file;
      } else {
        // get quota only when the node is a directory
        long nsQuota = -1L;
        if (layoutVersion <= -16) {
          nsQuota = in.readLong();
        }
        long dsQuota = -1L;
        if (layoutVersion <= -18) {
          dsQuota = in.readLong();
        }
        // read snapshot info
        boolean snapshottable = false;
        boolean withSnapshot = false;
        if (layoutVersion <= -42) {
          snapshottable = in.readBoolean();
          if (!snapshottable) {
            withSnapshot = in.readBoolean();
          }
        }
        
        PermissionStatus permissions = namesystem.getUpgradePermission();
        if (layoutVersion <= -11) {
          permissions = PermissionStatus.read(in);
        }
        //return
        final INodeDirectory dir = nsQuota >= 0 || dsQuota >= 0?
            new INodeDirectoryWithQuota(localName, permissions,
                modificationTime, nsQuota, dsQuota)
            : new INodeDirectory(localName, permissions, modificationTime);
        return snapshottable ? new INodeDirectorySnapshottable(dir)
            : withSnapshot ? new INodeDirectoryWithSnapshot(dir)
            : dir;
      }
    }
    
    private void loadFilesUnderConstruction(int version, DataInputStream in)
        throws IOException {
      if (version > -13) // pre lease image version
        return;
      int size = in.readInt();

      LOG.info("Number of files under construction = " + size);

      for (int i = 0; i < size; i++) {
        INodeFileUnderConstruction cons 
            = FSImageSerialization.readINodeUnderConstruction(in);

        // verify that file exists in namespace
        String path = cons.getLocalName();
        final INodesInPath iip = fsDir.getLastINodeInPath(path);
        INodeFile oldnode = INodeFile.valueOf(iip.getINode(0), path);
        cons.setLocalName(oldnode.getLocalNameBytes());
        cons.setParent(oldnode.getParent());
        
        if (oldnode instanceof INodeFileWithSnapshot) {
          cons = new INodeFileUnderConstructionWithSnapshot(cons,
              ((INodeFileWithSnapshot)oldnode).getDiffs());
        }
        
        fsDir.replaceINodeFile(path, oldnode, cons);
        namesystem.leaseManager.addLease(cons.clientName, path);
      }
    }

    private void loadSecretManagerState(int version, DataInputStream in,
        FSNamesystem fs) throws IOException {
      if (version > -19) {
        // SecretManagerState is not available.
        // This must not happen if security is turned on.
        return;
      }
      fs.loadSecretManagerState(in);
    }
    
    private void loadDatanodes(int version, DataInputStream in)
        throws IOException {
      if (version > -3) // pre datanode image version
        return;
      if (version <= -12) {
        return; // new versions do not store the datanodes any more.
      }
      int size = in.readInt();
      for(int i = 0; i < size; i++) {
        DatanodeImage nodeImage = new DatanodeImage();
        nodeImage.readFields(in);
        // We don't need to add these descriptors any more.
      }
    }
  }
  
  
  /**
   * A one-shot class responsible for writing an image file.
   * The write() function should be called once, after which the getter
   * functions may be used to retrieve information about the file that was written.
   */
  static class Saver {
    static private final byte[] PATH_SEPARATOR = DFSUtil
        .string2Bytes(Path.SEPARATOR);
    
    private final int namespaceID;
    
    Saver(int namespaceID) {
      this.namespaceID = namespaceID;
    }
    
    /**
     * Save the contents of the FS image to the file.
     */
    void save(File newFile) throws IOException {
      FSNamesystem fsNamesys = FSNamesystem.getFSNamesystem();
      FSDirectory fsDir = fsNamesys.dir;
      long startTime = FSNamesystem.now();
      //
      // Write out data
      //
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(
          new FileOutputStream(newFile)));
      try {
        out.writeInt(FSConstants.LAYOUT_VERSION);
        out.writeInt(namespaceID);
        out.writeLong(fsDir.rootDir.numItemsInTree());
        out.writeLong(fsNamesys.getGenerationStamp());
        fsNamesys.getSnapshotManager().write(out);
        byte[] byteStore = new byte[4 * FSConstants.MAX_PATH_LENGTH];
        ByteBuffer strbuf = ByteBuffer.wrap(byteStore);
        // save the root
        FSImageSerialization.saveINode2Image(fsDir.rootDir, out, false);
        // save the rest of the nodes
        saveImage(strbuf, fsDir.rootDir, out, null);
        fsNamesys.saveFilesUnderConstruction(out);
        fsNamesys.saveSecretManagerState(out);
        strbuf = null;
      } finally {
        out.close();
      }

      LOG.info("Image file of size " + newFile.length() + " saved in " 
          + (FSNamesystem.now() - startTime)/1000 + " seconds.");
    }
    
    /**
     * Save file tree image starting from the given root.
     * This is a recursive procedure, which first saves all children and 
     * snapshot diffs of a current directory and then moves inside the 
     * sub-directories.
     * 
     * @param currentDirName A ByteBuffer storing the path leading to the 
     *                       current node. For a snapshot node, the path is
     *                       (the snapshot path - ".snapshot/snapshot_name")
     * @param current The current node
     * @param out The DataoutputStream to write the image
     * @param snapshot The possible snapshot associated with the current node
     */
    private void saveImage(ByteBuffer currentDirName, INodeDirectory current,
        DataOutputStream out, Snapshot snapshot) throws IOException {
      final ReadOnlyList<INode> children = current.getChildrenList(null);
      int dirNum = 0;
      Map<Snapshot, List<INodeDirectory>> snapshotDirMap = null;
      if (current instanceof INodeDirectoryWithSnapshot) {
        snapshotDirMap = new HashMap<Snapshot, List<INodeDirectory>>();
        dirNum += ((INodeDirectoryWithSnapshot) current).
            getSnapshotDirectory(snapshotDirMap);
      }
      
      // 1. Print prefix (parent directory name)
      int prefixLen = currentDirName.position();
      if (snapshot == null) {
        if (prefixLen == 0) {  // root
          out.writeShort(PATH_SEPARATOR.length);
          out.write(PATH_SEPARATOR);
        } else {  // non-root directories
          out.writeShort(prefixLen);
          out.write(currentDirName.array(), 0, prefixLen);
        }
      } else {
        String nonSnapshotPath = prefixLen == 0 ? Path.SEPARATOR : DFSUtil
            .bytes2String(currentDirName.array(), 0, prefixLen);
        String snapshotFullPath = computeSnapshotPath(nonSnapshotPath, 
            snapshot);
        byte[] snapshotFullPathBytes = DFSUtil.string2Bytes(snapshotFullPath);
        out.writeShort(snapshotFullPathBytes.length);
        out.write(snapshotFullPathBytes);
      }
      
      // 2. Write INodeDirectorySnapshottable#snapshotsByNames to record all
      // Snapshots
      if (current instanceof INodeDirectorySnapshottable) {
        INodeDirectorySnapshottable snapshottableNode = 
            (INodeDirectorySnapshottable) current;
        SnapshotFSImageFormat.saveSnapshots(snapshottableNode, out);
      } else {
        out.writeInt(-1); // # of snapshots
      }

      // 3. Write children INode 
      dirNum += saveChildren(children, out);
      
      // 4. Write DirectoryDiff lists, if there is any.
      SnapshotFSImageFormat.saveDirectoryDiffList(current, out);
      
      // Write sub-tree of sub-directories, including possible snapshots of 
      // deleted sub-directories
      out.writeInt(dirNum); // the number of sub-directories
      for(INode child : children) {
        if(!child.isDirectory())
          continue;
        currentDirName.put(PATH_SEPARATOR).put(child.getLocalNameBytes());
        saveImage(currentDirName, child.asDirectory(), out, snapshot);
        currentDirName.position(prefixLen);
      }
      if (snapshotDirMap != null) {
        for (Snapshot ss : snapshotDirMap.keySet()) {
          List<INodeDirectory> snapshotSubDirs = snapshotDirMap.get(ss);
          for (INodeDirectory subDir : snapshotSubDirs) {
            currentDirName.put(PATH_SEPARATOR).put(subDir.getLocalNameBytes());
            saveImage(currentDirName, subDir, out, ss);
            currentDirName.position(prefixLen);
          }
        }
      }
    }
    
    /**
     * Save children INodes.
     * @param children The list of children INodes
     * @param out The DataOutputStream to write
     * @return Number of children that are directory
     */
    private int saveChildren(ReadOnlyList<INode> children, DataOutputStream out)
        throws IOException {
      // Write normal children INode. 
      out.writeInt(children.size());
      int dirNum = 0;
      for(INode child : children) {
        // print all children first
        FSImageSerialization.saveINode2Image(child, out, false);
        if (child.isDirectory()) {
          dirNum++;
        }
      }
      return dirNum;
    }
    
    /**
     * The nonSnapshotPath is a path without snapshot in order to enable buffer
     * reuse. If the snapshot is not null, we need to compute a snapshot path.
     * E.g., when nonSnapshotPath is "/test/foo/bar/" and the snapshot is s1 of
     * /test, we actually want to save image for directory /test/foo/bar/ under
     * snapshot s1 of /test, and the path to save thus should be
     * "/test/.snapshot/s1/foo/bar/".
     * 
     * @param nonSnapshotPath The path without snapshot related information.
     * @param snapshot The snapshot associated with the inode that the path 
     *                 actually leads to.
     * @return The snapshot path.                
     */
    private String computeSnapshotPath(String nonSnapshotPath, 
        Snapshot snapshot) {
      String snapshotParentFullPath = snapshot.getRoot().getParent()
          .getFullPathName();
      String snapshotName = snapshot.getRoot().getLocalName();
      String relativePath = nonSnapshotPath.equals(snapshotParentFullPath) ? 
          Path.SEPARATOR : nonSnapshotPath.substring(
               snapshotParentFullPath.length());
      String snapshotFullPath = snapshotParentFullPath + Path.SEPARATOR
          + HdfsConstants.DOT_SNAPSHOT_DIR + Path.SEPARATOR + snapshotName
          + relativePath;
      return snapshotFullPath;
    }
  }
}