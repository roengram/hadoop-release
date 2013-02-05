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
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/**
 * Directory INode class.
 */
public class INodeDirectory extends INode {
  /** Cast INode to INodeDirectory. */
  public static INodeDirectory valueOf(INode inode, Object path
      ) throws FileNotFoundException {
    if (inode == null) {
      throw new FileNotFoundException("Directory does not exist: " + path);
    }
    if (!inode.isDirectory()) {
      throw new FileNotFoundException("Is not a directory: "
          + DFSUtil.path2String(path));
    }
    return (INodeDirectory)inode; 
  }

  protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;
  final static String ROOT_NAME = "";

  private List<INode> children = null;

  public INodeDirectory(String name, PermissionStatus permissions) {
    super(name, permissions);
  }

  public INodeDirectory(PermissionStatus permissions, long mTime) {
    super(permissions, mTime, 0);
  }

  /** constructor */
  INodeDirectory(byte[] name, PermissionStatus permissions, long mtime) {
    super(name, permissions, null, mtime, 0L);
  }
  
  /** copy constructor
   * 
   * @param other
   */
  public INodeDirectory(INodeDirectory other, boolean adopt) {
    super(other);
    this.children = other.children;
    if (adopt && this.children != null) {
      for (INode child : children) {
        child.parent = this;
      }
    }
  }
  
  /** @return true unconditionally. */
  public final boolean isDirectory() {
    return true;
  }

  private void assertChildrenNonNull() {
    if (children == null) {
      throw new AssertionError("children is null: " + this);
    }
  }

  private int searchChildren(byte[] name) {
    return Collections.binarySearch(children, name);
  }

  protected int searchChildrenForExistingINode(final INode inode) {
    assertChildrenNonNull();
    final byte[] name = inode.getLocalNameBytes();
    final int i = searchChildren(name);
    if (i < 0) {
      throw new AssertionError("Child not found: name="
          + DFSUtil.bytes2String(name));
    }
    return i;
  }

  /** Is this a snapshottable directory? */
  public boolean isSnapshottable() {
    return false;
  }

  INode removeChild(INode node) {
    assertChildrenNonNull();
    final int i = searchChildren(node.getLocalNameBytes());
    return i >= 0? children.remove(i): null;
  }

  /**
   * Remove the specified child from this directory.
   * 
   * @param child the child inode to be removed
   * @param latest See {@link INode#recordModification(Snapshot)}.
   * @return the removed child inode.
   */
  public INode removeChild(INode child, Snapshot latest) {
    assertChildrenNonNull();

    if (latest != null) {
      final INodeDirectoryWithSnapshot dir = replaceSelf4INodeDirectoryWithSnapshot(latest);
      return dir.removeChild(child, latest);
    }

    final int i = searchChildren(child.getLocalNameBytes());
    return i >= 0? children.remove(i): null;
  }

  /**
   * Replace itself with {@link INodeDirectoryWithQuota} or
   * {@link INodeDirectoryWithSnapshot} depending on the latest snapshot.
   */
  INodeDirectoryWithQuota replaceSelf4Quota(final Snapshot latest,
      final long nsQuota, final long dsQuota) {
    if (this instanceof INodeDirectoryWithQuota) {
      throw new IllegalStateException(
          "this is already an INodeDirectoryWithQuota, this=" + this);
    }

    if (latest == null) {
      final INodeDirectoryWithQuota q = new INodeDirectoryWithQuota(
          this, true, nsQuota, dsQuota);
      replaceSelf(q);
      return q;
    } else {
      final INodeDirectoryWithSnapshot s
          = INodeDirectoryWithSnapshot.newInstance(this, null);
      s.setQuota(nsQuota, dsQuota, null);
      replaceSelf(s);
      s.saveSelf2Snapshot(latest, this);
      return s;
    }
  }
  
  /** Replace itself with an {@link INodeDirectorySnapshottable}. */
  public INodeDirectorySnapshottable replaceSelf4INodeDirectorySnapshottable(
      Snapshot latest) {
    final INodeDirectorySnapshottable s = new INodeDirectorySnapshottable(this);
    replaceSelf(s);
    s.saveSelf2Snapshot(latest, this);
    return s;
  }

  /** Replace itself with an {@link INodeDirectoryWithSnapshot}. */
  public INodeDirectoryWithSnapshot replaceSelf4INodeDirectoryWithSnapshot(
      Snapshot latest) {
    if (this instanceof INodeDirectoryWithSnapshot) {
      throw new IllegalStateException(
          "this is already an INodeDirectoryWithSnapshot, this=" + this);
    }

    final INodeDirectoryWithSnapshot withSnapshot
        = INodeDirectoryWithSnapshot.newInstance(this, latest);
    replaceSelf(withSnapshot);
    return withSnapshot;
  }

  /** Replace itself with {@link INodeDirectory}. */
  public INodeDirectory replaceSelf4INodeDirectory() {
    if (getClass() == INodeDirectory.class) {
      throw new IllegalStateException(
          "the class is already INodeDirectory, this=" + this);
    }

    final INodeDirectory newNode = new INodeDirectory(this, true);
    replaceSelf(newNode);
    return newNode;
  }

  /** Replace itself with the given directory. */
  private final void replaceSelf(INodeDirectory newDir) {
    final INodeDirectory parent = getParent();
    if (parent == null) {
      throw new IllegalStateException("parent is null, this=" + this);
    }

    final int i = parent.searchChildrenForExistingINode(newDir);
    final INode oldDir = parent.children.set(i, newDir);
    oldDir.setParent(null);
  }
  
  /** Replace a child {@link INodeFile} with an {@link INodeFileWithSnapshot}. */
  INodeFileWithSnapshot replaceINodeFile(final INodeFile child) {
    assertChildrenNonNull();
    if (child instanceof INodeFileWithSnapshot) {
      throw new IllegalStateException(
          "Child file is already an INodeFileWithLink, child=" + child);
    }

    final INodeFileWithSnapshot newChild = new INodeFileWithSnapshot(child);
    final int i = searchChildrenForExistingINode(newChild);
    children.set(i, newChild);
    return newChild;
  }
  
  @Override
  public INodeDirectory recordModification(Snapshot latest) {
    if (latest == null) {
      return this;
    }
    final INodeDirectoryWithSnapshot withSnapshot
        = replaceSelf4INodeDirectoryWithSnapshot(latest);
    withSnapshot.saveSelf2Snapshot(latest, this);
    return withSnapshot;
  }

  /**
   * Save the child to the latest snapshot.
   * 
   * @return the child inode, which may be replaced.
   */
  public INode saveChild2Snapshot(INode child, Snapshot latest) {
    if (latest == null) {
      return child;
    }
    return replaceSelf4INodeDirectoryWithSnapshot(latest)
        .saveChild2Snapshot(child, latest);
  }

  /**
   * @param name the name of the child
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current directory.
   * @return the child inode.
   */
  public INode getChild(byte[] name, Snapshot snapshot) {
    final ReadOnlyList<INode> c = getChildrenList(snapshot);
    final int i = ReadOnlyList.Util.binarySearch(c, name);
    return i < 0 ? null : c.get(i);
  }

  /** @return the {@link INodesInPath} containing only the last inode. */
  INodesInPath getLastINodeInPath(String path) {
    return getExistingPathINodes(getPathComponents(path), 1);
  }

  /** @return the {@link INodesInPath} containing all inodes in the path. */
  INodesInPath getINodesInPath(String path) {
    final byte[][] components = getPathComponents(path);
    return getExistingPathINodes(components, components.length);
  }
  
  /** @return the last inode in the path. */
  INode getNode(String path) {
    return getLastINodeInPath(path).getINode(0);
  }
  
  /**
   * @return the INode of the last component in src, or null if the last
   * component does not exist.
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  INode getINode4Write(String src) throws SnapshotAccessControlException {
    return getINodesInPath4Write(src).getLastINode();
  }

  /**
   * @return the INodesInPath of the components in src
   * @throws SnapshotAccessControlException if path is in RO snapshot
   */
  INodesInPath getINodesInPath4Write(String src)
      throws SnapshotAccessControlException {
    final byte[][] components = INode.getPathComponents(src);
    INodesInPath inodesInPath = getExistingPathINodes(components,
        components.length);
    if (inodesInPath.isSnapshot()) {
      throw new SnapshotAccessControlException(
          "Modification on read-only snapshot is disallowed");
    }
    return inodesInPath;
  }

  /**
   * Retrieve existing INodes from a path. If existing is big enough to store
   * all path components (existing and non-existing), then existing INodes
   * will be stored starting from the root INode into existing[0]; if
   * existing is not big enough to store all path components, then only the
   * last existing and non existing INodes will be stored so that
   * existing[existing.length-1] refers to the target INode.
   * 
   * <p>
   * Example: <br>
   * Given the path /c1/c2/c3 where only /c1/c2 exists, resulting in the
   * following path components: ["","c1","c2","c3"],
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?])</code> should fill the
   * array with [c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?])</code> should fill the
   * array with [null]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?])</code> should fill the
   * array with [c1,c2] <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?])</code> should fill
   * the array with [c2,null]
   * 
   * <p>
   * <code>getExistingPathINodes(["","c1","c2"], [?,?,?,?])</code> should fill
   * the array with [rootINode,c1,c2,null], <br>
   * <code>getExistingPathINodes(["","c1","c2","c3"], [?,?,?,?])</code> should
   * fill the array with [rootINode,c1,c2,null]
   * @param components array of path component name
   * @param numOfINodes number of INodes to return
   * @return number of existing INodes in the path
   */
  INodesInPath getExistingPathINodes(byte[][] components, int numOfINodes) {
    assert this.compareTo(components[0]) == 0 :
      "Incorrect name " + getLocalName() + " expected "
      + (components[0] == null? null: DFSUtil.bytes2String(components[0]));

    INodesInPath existing = new INodesInPath(components, numOfINodes);
    INode curNode = this;
    int count = 0;
    int index = numOfINodes - components.length;
    if (index > 0)
      index = 0;
    while ((count < components.length) && (curNode != null)) {
      if (index >= 0) {
        existing.addNode(curNode);
      }
      if (curNode instanceof INodeDirectorySnapshottable) {
        //if the path is a non-snapshot path, update the latest snapshot.
        if (!existing.isSnapshot()) {
          existing.updateLatestSnapshot(
              ((INodeDirectorySnapshottable)curNode).getLastSnapshot());
        }
      }
      if (!curNode.isDirectory() || (count == components.length - 1))
        break; // no more child, stop here
      final INodeDirectory parentDir = (INodeDirectory)curNode;
      final byte[] childName = components[count + 1];
      
      // check if the next byte[] in components is for ".snapshot"
      if (isDotSnapshotDir(childName)
          && (curNode instanceof INodeDirectorySnapshottable)) {
        // skip the ".snapshot" in components
        count++;
        index++;
        existing.isSnapshot = true;
        if (index >= 0) { // decrease the capacity by 1 to account for .snapshot
          existing.capacity--;
        }
        // check if ".snapshot" is the last element of components
        if (count == components.length - 1) {
          break;
        }
        // Resolve snapshot root
        final Snapshot s = ((INodeDirectorySnapshottable) parentDir)
            .getSnapshot(components[count + 1]);
        if (s == null) {
          // snapshot not found
          curNode = null;
        } else {
          curNode = s.getRoot();
          existing.setSnapshot(s);
        }
        if (index >= -1) {
          existing.snapshotRootIndex = existing.numNonNull;
        }
      } else {
        // normal case, and also for resolving file/dir under snapshot root
        curNode = parentDir.getChild(childName, existing.getPathSnapshot());
      }
      count += 1;
      index += 1;
    }
    return existing;
  }

  /**
   * @return true if path component is {@link HdfsConstants#DOT_SNAPSHOT_DIR}
   */
  private static boolean isDotSnapshotDir(byte[] pathComponent) {
    return pathComponent == null ? false : HdfsConstants.DOT_SNAPSHOT_DIR
        .equalsIgnoreCase(DFSUtil.bytes2String(pathComponent));
  }

  /**
   * Add a child inode to the directory.
   * 
   * @param node INode to insert
   * @param inheritPermission inherit permission from parent?
   * @return  false if the child with this name already exists; 
   *          otherwise, return true
   */
  public boolean addChild(final INode node, boolean inheritPermission,
      final Snapshot latest) {
    if (latest != null) {
      final INodeDirectoryWithSnapshot dir = replaceSelf4INodeDirectoryWithSnapshot(latest);
      return dir.addChild(node, inheritPermission, latest);
    }

    if (children == null) {
      children = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
    }
    final int low = searchChildren(node.getLocalNameBytes());
    if(low >= 0)
      return false;
    node.parent = this;
    children.add(-low - 1, node);
    
    if (inheritPermission) {
      FsPermission p = getFsPermission();
      //make sure the  permission has wx for the user
      if (!p.getUserAction().implies(FsAction.WRITE_EXECUTE)) {
        p = new FsPermission(p.getUserAction().or(FsAction.WRITE_EXECUTE),
            p.getGroupAction(), p.getOtherAction());
      }
      node.setPermission(p, latest);
    }
    
    // update modification time of the parent directory
    updateModificationTime(node.getModificationTime(), latest);
    if (node.getGroupName() == null) {
      node.setGroup(getGroupName(), latest);
    }
    return true;
  }

  /**
   * Given a child's name, return the index of the next child
   * 
   * @param name a child's name
   * @return the index of the next child
   */
  static int nextChild(ReadOnlyList<INode> children, byte[] name) {
    if (name.length == 0) { // empty name
      return 0;
    }
    int nextPos = ReadOnlyList.Util.binarySearch(children, name) + 1;
    if (nextPos >= 0) {
      return nextPos;
    }
    return -nextPos;
  }
  
  /**
   * Add new INode to the file tree.
   * Find the parent and insert 
   * 
   * @param path file path
   * @param newNode INode to be added
   * @param inheritPermission If true, copy the parent's permission to newNode.
   * @return null if the node already exists; inserted INode, otherwise
   * @throws FileNotFoundException if parent does not exist or 
   * is not a directory.
   */
  boolean addINode(String path, INode newNode, boolean inheritPermission
      ) throws FileNotFoundException {
    if(addToParent(path, newNode, null, inheritPermission) == null)
      return false;
    return true;
  }

  /**
   * Add new inode to the parent if specified.
   * Optimized version of addNode() if parent is not null.
   * 
   * @return  parent INode if new inode is inserted
   *          or null if it already exists.
   * @throws  FileNotFoundException if parent does not exist or 
   *          is not a directory.
   */
  <T extends INode> INodeDirectory addToParent(
                                      String path,
                                      T newNode,
                                      INodeDirectory parent,
                                      boolean inheritPermission
                                    ) throws FileNotFoundException {
    byte[][] pathComponents = getPathComponents(path);
    assert pathComponents != null : "Incorrect path " + path;
    int pathLen = pathComponents.length;
    if (pathLen < 2)  // add root
      return null;
    if(parent == null) {
      // Gets the parent INode
      INodesInPath inodes =  getExistingPathINodes(pathComponents, 2);
      INode inode = inodes.inodes[0];
      if (inode == null) {
        throw new FileNotFoundException("Parent path does not exist: "+path);
      }
      if (!inode.isDirectory()) {
        throw new FileNotFoundException("Parent path is not a directory: "+path);
      }
      parent = (INodeDirectory)inode;
    }
    // insert into the parent children list
    newNode.setLocalName(pathComponents[pathComponents.length - 1]);
    if(!parent.addChild(newNode, inheritPermission, null))
      return null;
    return parent;
  }

  /** {@inheritDoc} */
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += 1;
    if (children != null) {
      for (INode child : children) {
        child.spaceConsumedInTree(counts);
      }
    }
    return counts;    
  }

  /** {@inheritDoc} */
  long[] computeContentSummary(long[] summary) {
    // Walk through the children of this node, using a new summary array
    // for the (sub)tree rooted at this node
    assert 4 == summary.length;
    long[] subtreeSummary = new long[]{0,0,0,0};
    if (children != null) {
      for (INode child : children) {
        child.computeContentSummary(subtreeSummary);
      }
    }
    if (this instanceof INodeDirectoryWithQuota) {
      // Warn if the cached and computed diskspace values differ
      INodeDirectoryWithQuota node = (INodeDirectoryWithQuota)this;
      long space = node.diskspaceConsumed();
      assert -1 == node.getDsQuota() || space == subtreeSummary[3];
      if (-1 != node.getDsQuota() && space != subtreeSummary[3]) {
        NameNode.LOG.warn("Inconsistent diskspace for directory "
          +getLocalName()+". Cached: "+space+" Computed: "+subtreeSummary[3]);
      }
    }

    // update the passed summary array with the values for this node's subtree
    for (int i = 0; i < summary.length; i++) {
      summary[i] += subtreeSummary[i];
    }

    summary[2]++;
    return summary;
  }

  /**
   * @param snapshot
   *          if it is not null, get the result from the given snapshot;
   *          otherwise, get the result from the current directory.
   * @return the current children list if the specified snapshot is null;
   *         otherwise, return the children list corresponding to the snapshot.
   *         Note that the returned list is never null.
   */
  public ReadOnlyList<INode> getChildrenList(final Snapshot snapshot) {
    return children == null ? EMPTY_READ_ONLY_LIST : ReadOnlyList.Util
        .asReadOnlyList(children);
  }
  
  /** Set the children list. */
  public void setChildren(List<INode> children) {
    this.children = children;
  }

  @Override
  int collectSubtreeBlocksAndClear(BlocksMapUpdateInfo info) {
    int total = 1;
    if (children == null) {
      return total;
    }
    for (INode child : children) {
      total += child.collectSubtreeBlocksAndClear(info);
    }
    parent = null;
    children = null;
    return total;
  }
  
  /**
   * Used by
   * {@link INodeDirectory#getExistingPathINodes(byte[][], int, boolean)}.
   * Contains INodes information resolved from a given path.
   */
  public static class INodesInPath {
    private final byte[][] path;
    /**
     * Array with the specified number of INodes resolved for a given path.
     */
    private INode[] inodes;
    
    /**
     * Indicate the number of non-null elements in {@link #inodes}
     */
    private int numNonNull;
    /**
     * The path for a snapshot file/dir contains the .snapshot thus makes the
     * length of the path components larger the number of inodes. We use
     * the capacity to control this special case.
     */
    private int capacity;
    /**
     * true if this path corresponds to a snapshot
     */
    private boolean isSnapshot;
    /**
     * Index of {@link INodeDirectoryWithSnapshot} for snapshot path, else -1
     */
    private int snapshotRootIndex;
    /**
     * For snapshot paths, it is the reference to the snapshot; or null if the
     * snapshot does not exist. For non-snapshot paths, it is the reference to
     * the latest snapshot found in the path; or null if no snapshot is found.
     */
    private Snapshot snapshot = null; 
    
    public INodesInPath(byte[][] path, int number) {
      this.path = path;
      assert (number >= 0);
      inodes = new INode[number];
      capacity = number;
      numNonNull = 0;
      isSnapshot = false;
      snapshotRootIndex = -1;
    }
    
    /**
     * For non-snapshot paths, return the latest snapshot found in the path.
     * For snapshot paths, return null.
     */
    public Snapshot getLatestSnapshot() {
      return isSnapshot? null: snapshot;
    }
    
    /**
     * For snapshot paths, return the snapshot specified in the path. For
     * non-snapshot paths, return null.
     */
    public Snapshot getPathSnapshot() {
      return isSnapshot? snapshot: null;
    }

    private void setSnapshot(Snapshot s) {
      snapshot = s;
    }

    private void updateLatestSnapshot(Snapshot s) {
      if (Snapshot.ID_COMPARATOR.compare(snapshot, s) < 0) {
        snapshot = s;
      }
    }
    
    /**
     * @return the whole inodes array including the null elements.
     */
    INode[] getINodes() {
      if (capacity < inodes.length) {
        INode[] newNodes = new INode[capacity];
        for (int i = 0; i < capacity; i++) {
          newNodes[i] = inodes[i];
        }
        inodes = newNodes;
      }
      return inodes;
    }
    
    /** @return the i-th inode. */
    public INode getINode(int i) {
      return inodes[i];
    }
    
    /** @return the last inode. */
    public INode getLastINode() {
      return inodes[inodes.length - 1];
    }
    
    byte[] getLastLocalName() {
      return path[path.length - 1];
    }
    
    /**
     * @return index of the {@link INodeDirectoryWithSnapshot} in
     *         {@link #inodes} for snapshot path, else -1.
     */
    int getSnapshotRootIndex() {
      return this.snapshotRootIndex;
    }

    /**
     * @return isSnapshot true for a snapshot path
     */
    boolean isSnapshot() {
      return this.isSnapshot;
    }

    /**
     * Add an INode at the end of the array
     */
    private void addNode(INode node) {
      inodes[numNonNull++] = node;
    }

    /**
     * @return The number of non-null elements
     */
    int getNumNonNull() {
      return numNonNull;
    }
    
    static String toString(INode inode) {
      return inode == null ? null : inode.getLocalName();
    }

    @Override
    public String toString() {
      return toString(true);
    }
    
    private String toString(boolean validateObject) {
      if (validateObject) {
        validate();
      }
      final StringBuilder b = new StringBuilder(getClass().getSimpleName())
          .append(": path = ").append(DFSUtil.byteArray2PathString(path))
          .append("\n  inodes = ");
      if (inodes == null) {
        b.append("null");
      } else if (inodes.length == 0) {
        b.append("[]");
      } else {
        b.append("[").append(toString(inodes[0]));
        for (int i = 1; i < inodes.length; i++) {
          b.append(", ").append(toString(inodes[i]));
        }
        b.append("], length=").append(inodes.length);
      }
      b.append("\n  numNonNull = ").append(numNonNull)
          .append("\n  capacity   = ").append(capacity)
          .append("\n  isSnapshot        = ").append(isSnapshot)
          .append("\n  snapshotRootIndex = ").append(snapshotRootIndex)
          .append("\n  snapshot          = ").append(snapshot);
      return b.toString();
    }
    
    void validate() {
      // check parent up to snapshotRootIndex or numNonNull
      final int n = snapshotRootIndex >= 0? snapshotRootIndex + 1: numNonNull;  
      int i = 0;
      if (inodes[i] != null) {
        for(i++; i < n && inodes[i] != null; i++) {
          final INodeDirectory parent_i = inodes[i].getParent();
          final INodeDirectory parent_i_1 = inodes[i-1].getParent();
          if (parent_i != inodes[i-1] &&
              (parent_i_1 == null || !parent_i_1.isSnapshottable()
                  || parent_i != parent_i_1)) {
            throw new AssertionError(
                "inodes[" + i + "].getParent() != inodes[" + (i-1)
                + "]\n  inodes[" + i + "]=" + inodes[i].toDetailString()
                + "\n  inodes[" + (i-1) + "]=" + inodes[i-1].toDetailString()
                + "\n this=" + toString(false));
          }
        }
      }
      if (i != n) {
        throw new AssertionError("i = " + i + " != " + n
            + ", this=" + toString(false));
      }
    }
    
    void setINode(int i, INode inode) {
      inodes[i] = inode;
    }
  }

  /*
   * The following code is to dump the tree recursively for testing.
   * 
   *      \- foo   (INodeDirectory@33dd2717)
   *        \- sub1   (INodeDirectory@442172)
   *          +- file1   (INodeFile@78392d4)
   *          +- file2   (INodeFile@78392d5)
   *          +- sub11   (INodeDirectory@8400cff)
   *            \- file3   (INodeFile@78392d6)
   *          \- z_file4   (INodeFile@45848712)
   */
  static final String DUMPTREE_EXCEPT_LAST_ITEM = "+-"; 
  static final String DUMPTREE_LAST_ITEM = "\\-";
  @Override
  public void dumpTreeRecursively(PrintWriter out, StringBuilder prefix,
      final Snapshot snapshot) {
    super.dumpTreeRecursively(out, prefix, snapshot);
    if (prefix.length() >= 2) {
      prefix.setLength(prefix.length() - 2);
      prefix.append("  ");
    }
    dumpTreeRecursively(out, prefix,
        new Iterable<Pair<? extends INode, Snapshot>>() {
      final Iterator<INode> i = getChildrenList(snapshot).iterator();
      
      @Override
      public Iterator<Pair<? extends INode, Snapshot>> iterator() {
        return new Iterator<Pair<? extends INode, Snapshot>>() {
          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public Pair<INode, Snapshot> next() {
            return new Pair<INode, Snapshot>(i.next(), snapshot);
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    });
  }

  /**
   * Dump the given subtrees.
   * @param prefix The prefix string that each line should print.
   * @param subs The subtrees.
   */
  protected static void dumpTreeRecursively(PrintWriter out,
      StringBuilder prefix, Iterable<Pair<? extends INode, Snapshot>> subs) {
    if (subs != null) {
      for(final Iterator<Pair<? extends INode, Snapshot>> i = subs.iterator(); i.hasNext();) {
        final Pair<? extends INode, Snapshot> pair = i.next();
        prefix.append(i.hasNext() ? DUMPTREE_EXCEPT_LAST_ITEM
            : DUMPTREE_LAST_ITEM);
        pair.left.dumpTreeRecursively(out, prefix, pair.right);
        prefix.setLength(prefix.length() - 2);
      }
    }
  }
}
