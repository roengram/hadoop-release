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
import java.util.Map;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileUnderConstructionWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeFileWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

import com.google.common.base.Preconditions;

/**
 * Directory INode class.
 */
public class INodeDirectory extends INodeWithAdditionalFields {
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
    return inode.asDirectory(); 
  }

  protected static final int DEFAULT_FILES_PER_DIRECTORY = 5;
  final static byte[] ROOT_NAME = DFSUtil.string2Bytes("");

  private List<INode> children = null;
  
  /** constructor */
  public INodeDirectory(long id, byte[] name, PermissionStatus permissions,
      long mtime) {
    super(id, name, permissions, mtime, 0L);
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
        child.setParent(this);
      }
    }
  }
  
  /** @return true unconditionally. */
  public final boolean isDirectory() {
    return true;
  }

  private int searchChildren(byte[] name) {
    return children == null? -1: Collections.binarySearch(children, name);
  }

  /** @return this object. */
  @Override
  public final INodeDirectory asDirectory() {
    return this;
  }

  /** Is this a snapshottable directory? */
  public boolean isSnapshottable() {
    return false;
  }

  /**
   * Remove the specified child from this directory.
   * 
   * @param child the child inode to be removed
   * @param latest See {@link INode#recordModification(Snapshot, INodeMap)}.
   */
  public boolean removeChild(INode child, Snapshot latest,
      final INodeMap inodeMap) throws QuotaExceededException {
    if (isInLatestSnapshot(latest)) {
      return replaceSelf4INodeDirectoryWithSnapshot(inodeMap)
          .removeChild(child, latest, inodeMap);
    }

    return removeChild(child);
  }

  /** 
   * Remove the specified child from this directory.
   * The basic remove method which actually calls children.remove(..).
   *
   * @param child the child inode to be removed
   * 
   * @return true if the child is removed; false if the child is not found.
   */
  protected final boolean removeChild(final INode child) {
    final int i = searchChildren(child.getLocalNameBytes());
    if (i < 0) {
      return false;
    }

    final INode removed = children.remove(i);
    if (removed != child) {
      throw new IllegalStateException("removed != child. removed = "
          + removed.toDetailString() + ". child = "
          + child.toDetailString());
    }
    return true;
  }

  /**
   * Replace itself with {@link INodeDirectoryWithQuota} or
   * {@link INodeDirectoryWithSnapshot} depending on the latest snapshot.
   */
  INodeDirectoryWithQuota replaceSelf4Quota(final Snapshot latest,
      final long nsQuota, final long dsQuota, final INodeMap inodeMap)
      throws QuotaExceededException {
    if (this instanceof INodeDirectoryWithQuota) {
      throw new IllegalStateException(
          "this is already an INodeDirectoryWithQuota, this=" + this);
    }

    if (!this.isInLatestSnapshot(latest)) {
      final INodeDirectoryWithQuota q = new INodeDirectoryWithQuota(
          this, true, nsQuota, dsQuota);
      replaceSelf(q, inodeMap);
      return q;
    } else {
      final INodeDirectoryWithSnapshot s = new INodeDirectoryWithSnapshot(this);
      s.setQuota(nsQuota, dsQuota);
      return replaceSelf(s, inodeMap).saveSelf2Snapshot(latest, this);
    }
  }
  
  /** Replace itself with an {@link INodeDirectorySnapshottable}. */
  public INodeDirectorySnapshottable replaceSelf4INodeDirectorySnapshottable(
      Snapshot latest, final INodeMap inodeMap) throws QuotaExceededException {
    if (this instanceof INodeDirectorySnapshottable) {
      throw new IllegalStateException(
          "this is already an INodeDirectorySnapshottable, this=" + this);
    }
    final INodeDirectorySnapshottable s = new INodeDirectorySnapshottable(this);
    replaceSelf(s, inodeMap).saveSelf2Snapshot(latest, this);
    return s;
  }

  /** Replace itself with an {@link INodeDirectoryWithSnapshot}. */
  public INodeDirectoryWithSnapshot replaceSelf4INodeDirectoryWithSnapshot(
      final INodeMap inodeMap) {
    return replaceSelf(new INodeDirectoryWithSnapshot(this), inodeMap);
  }

  /** Replace itself with {@link INodeDirectory}. */
  public INodeDirectory replaceSelf4INodeDirectory(final INodeMap inodeMap) {
    if (getClass() == INodeDirectory.class) {
      throw new IllegalStateException(
          "the class is already INodeDirectory, this=" + this);
    }
    return replaceSelf(new INodeDirectory(this, true), inodeMap);
  }

  /** Replace itself with the given directory. */
  private final <N extends INodeDirectory> N replaceSelf(final N newDir,
      final INodeMap inodeMap) {
    final INodeReference ref = getParentReference();
    if (ref != null) {
      ref.setReferredINode(newDir);
      if (inodeMap != null) {
        inodeMap.put(newDir);
      }
    } else {
      final INodeDirectory parent = getParent();
      if (parent == null) {
        throw new IllegalStateException("parent is null, this=" + this);
      }
      parent.replaceChild(this, newDir, inodeMap);
    }
    clear();
    return newDir;
  }
  
  /** Replace the given child with a new child. */
  public void replaceChild(INode oldChild, final INode newChild,
      final INodeMap inodeMap) {
    if (children == null) {
      throw new IllegalStateException("children is null, this=" + this);
    }
    final int i = searchChildren(newChild.getLocalNameBytes());
    if (i < 0) {
      throw new IllegalStateException("cannot find a child with name "
          + newChild.getLocalName() + ", this=" + this);
    }
    Preconditions.checkState(oldChild == children.get(i)
        || oldChild == children.get(i).asReference().getReferredINode()
            .asReference().getReferredINode());
    oldChild = children.get(i);
    
    if (oldChild.isReference() && !newChild.isReference()) {
      // replace the referred inode, e.g., 
      // INodeFileWithSnapshot -> INodeFileUnderConstructionWithSnapshot
      final INode withCount = oldChild.asReference().getReferredINode();
      withCount.asReference().setReferredINode(newChild);
    } else {
      if (oldChild.isReference()) {
        // both are reference nodes, e.g., DstReference -> WithName
        final INodeReference.WithCount withCount = 
            (WithCount) oldChild.asReference().getReferredINode();
        withCount.removeReference(oldChild.asReference());
      }
      children.set(i, newChild);
    }
    // update the inodeMap
    if (inodeMap != null) {
      inodeMap.put(newChild);
    }
  }

  INodeReference.WithName replaceChild4ReferenceWithName(INode oldChild,
      Snapshot latest) {
    if (latest == null) {
      throw new IllegalArgumentException(
          "The latest snapshot is null in replaceChild4ReferenceWithName");
    }
    if (oldChild instanceof INodeReference.WithName) {
      return (INodeReference.WithName)oldChild;
    }

    final INodeReference.WithCount withCount;
    if (oldChild.isReference()) {
      Preconditions.checkState(oldChild instanceof INodeReference.DstReference);
      withCount = (INodeReference.WithCount) oldChild.asReference()
          .getReferredINode();
    } else {
      withCount = new INodeReference.WithCount(null, oldChild);
    }
    final INodeReference.WithName ref = new INodeReference.WithName(this,
        withCount, oldChild.getLocalNameBytes(), latest.getId());
    replaceChild(oldChild, ref, null);
    return ref;
  }
  
  private void replaceChildFile(final INodeFile oldChild, 
      final INodeFile newChild, final INodeMap inodeMap) {
    replaceChild(oldChild, newChild, inodeMap);
    newChild.updateINodeForBlocks();
  }
  
  /** Replace a child {@link INodeFile} with an {@link INodeFileWithSnapshot}. */
  INodeFileWithSnapshot replaceChild4INodeFileWithSnapshot(
      final INodeFile child, final INodeMap inodeMap) {
    if (child instanceof INodeFileWithSnapshot) {
      throw new IllegalStateException(
          "Child file is already an INodeFileWithLink, child=" + child);
    }
    final INodeFileWithSnapshot newChild = new INodeFileWithSnapshot(child);
    replaceChildFile(child, newChild, inodeMap);
    return newChild;
  }
  
  /**
   * Replace a child {@link INodeFile} with an
   * {@link INodeFileUnderConstructionWithSnapshot}.
   */
  INodeFileUnderConstructionWithSnapshot replaceChild4INodeFileUcWithSnapshot(
      final INodeFileUnderConstruction child, final INodeMap inodeMap) {
    if (child instanceof INodeFileUnderConstructionWithSnapshot) {
      throw new IllegalArgumentException(
      "Child file is already an INodeFileUnderConstructionWithSnapshot, child="
          + child);
    }
    final INodeFileUnderConstructionWithSnapshot newChild
        = new INodeFileUnderConstructionWithSnapshot(child, null);
    replaceChildFile(child, newChild, inodeMap);
    return newChild;
  }
  
  @Override
  public INodeDirectory recordModification(Snapshot latest,
      final INodeMap inodeMap) throws QuotaExceededException {
    return isInLatestSnapshot(latest) ? 
        replaceSelf4INodeDirectoryWithSnapshot(
        inodeMap).recordModification(latest, inodeMap) : this;
  }

  /**
   * Save the child to the latest snapshot.
   * 
   * @return the child inode, which may be replaced.
   */
  public INode saveChild2Snapshot(final INode child, final Snapshot latest,
      final INode snapshotCopy, final INodeMap inodeMap)
      throws QuotaExceededException {
    if (latest == null) {
      return child;
    }
    return replaceSelf4INodeDirectoryWithSnapshot(inodeMap)
        .saveChild2Snapshot(child, latest, snapshotCopy, inodeMap);
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
    return INodesInPath.resolve(this, getPathComponents(path), 1);
  }

  /** @return the {@link INodesInPath} containing all inodes in the path. */
  INodesInPath getINodesInPath(String path) {
    final byte[][] components = getPathComponents(path);
    return INodesInPath.resolve(this, components, components.length);
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
    INodesInPath inodesInPath = INodesInPath.resolve(this, components,
        components.length);
    if (inodesInPath.isSnapshot()) {
      throw new SnapshotAccessControlException(
          "Modification on read-only snapshot is disallowed");
    }
    return inodesInPath;
  }

  /**
   * Add a child inode to the directory.
   * 
   * @param node INode to insert
   * @param inheritPermission inherit permission from parent?
   * @param inodeMap update the inodeMap if the directory node gets replaced
   * @return  false if the child with this name already exists; 
   *          otherwise, return true
   */
  public boolean addChild(INode node, boolean inheritPermission,
      final Snapshot latest, final INodeMap inodeMap)
      throws QuotaExceededException {
    final int low = searchChildren(node.getLocalNameBytes());
    if (low >= 0) {
      return false;
    }

    if (isInLatestSnapshot(latest)) {
      return replaceSelf4INodeDirectoryWithSnapshot(inodeMap)
          .addChild(node, inheritPermission, latest, inodeMap);
    }
    
    addChild(node, low);
    
    if (inheritPermission) {
      FsPermission p = getFsPermission();
      //make sure the  permission has wx for the user
      if (!p.getUserAction().implies(FsAction.WRITE_EXECUTE)) {
        p = new FsPermission(p.getUserAction().or(FsAction.WRITE_EXECUTE),
            p.getGroupAction(), p.getOtherAction());
      }
      node.setPermission(p, latest, inodeMap);
    }
    
    // update modification time of the parent directory
    updateModificationTime(node.getModificationTime(), latest, inodeMap);
    return true;
  }

  /** The same as addChild(node, false, null, false) */
  public boolean addChild(INode node) {
    final int low = searchChildren(node.getLocalNameBytes());
    if (low >= 0) {
      return false;
    }
    addChild(node, low);
    return true;
  }

  /**
   * Add the node to the children list at the given insertion point.
   * The basic add method which actually calls children.add(..).
   */
  private void addChild(final INode node, final int insertionPoint) {
    if (children == null) {
      children = new ArrayList<INode>(DEFAULT_FILES_PER_DIRECTORY);
    }
    node.setParent(this);
    children.add(-insertionPoint - 1, node);

    if (node.getGroupName() == null) {
      node.setGroup(getGroupName());
    }
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

  @Override
  public Quota.Counts computeQuotaUsage(Quota.Counts counts, boolean useCache,
      int lastSnapshotId) {
    if (children != null) {
      for (INode child : children) {
        child.computeQuotaUsage(counts, useCache, lastSnapshotId);
      }
    }
    return computeQuotaUsage4CurrentDirectory(counts);
  }
  
  /** Add quota usage for this inode excluding children. */
  public Quota.Counts computeQuotaUsage4CurrentDirectory(Quota.Counts counts) {
    counts.add(Quota.NAMESPACE, 1);
    return counts;
  }

  @Override
  public Content.Counts computeContentSummary(final Content.Counts counts) {
    for (INode child : getChildrenList(null)) {
      child.computeContentSummary(counts);
    }
    counts.add(Content.DIRECTORY, 1);
    return counts;
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
    return children == null ? ReadOnlyList.Util.<INode> emptyList()
        : ReadOnlyList.Util.asReadOnlyList(children);
  }

  /** Set the children list to null. */
  public void clearChildren() {
    children = null;
  }

  @Override
  public void clear() {
    super.clear();
    clearChildren();
  }

  /** Call cleanSubtree(..) recursively down the subtree. */
  public Quota.Counts cleanSubtreeRecursively(final Snapshot snapshot,
      Snapshot prior, final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, final Map<INode, INode> excludedNodes, 
      final boolean countDiffChange) throws QuotaExceededException {
    Quota.Counts counts = Quota.Counts.newInstance();
    // in case of deletion snapshot, since this call happens after we modify
    // the diff list, the snapshot to be deleted has been combined or renamed
    // to its latest previous snapshot. (besides, we also need to consider nodes
    // created after prior but before snapshot. this will be done in 
    // INodeDirectoryWithSnapshot#cleanSubtree)
    Snapshot s = snapshot != null && prior != null ? prior : snapshot;
    for (INode child : getChildrenList(s)) {
      if (snapshot != null && excludedNodes != null
          && excludedNodes.containsKey(child)) {
        continue;
      } else {
        Quota.Counts childCounts = child.cleanSubtree(snapshot, prior,
            collectedBlocks, removedINodes, countDiffChange);
        counts.add(childCounts);
      }
    }
    return counts;
  }

  @Override
  public void destroyAndCollectBlocks(final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes) {
    for (INode child : getChildrenList(null)) {
      child.destroyAndCollectBlocks(collectedBlocks, removedINodes);
    }
    clear();
    removedINodes.add(this);
  }
  
  @Override
  public Quota.Counts cleanSubtree(final Snapshot snapshot, Snapshot prior,
      final BlocksMapUpdateInfo collectedBlocks,
      final List<INode> removedINodes, final boolean countDiffChange)
      throws QuotaExceededException {
    if (prior == null && snapshot == null) {
      // destroy the whole subtree and collect blocks that should be deleted
      Quota.Counts counts = Quota.Counts.newInstance();
      this.computeQuotaUsage(counts, true);
      destroyAndCollectBlocks(collectedBlocks, removedINodes);
      return counts;
    } else {
      // process recursively down the subtree
      Quota.Counts counts = cleanSubtreeRecursively(snapshot, prior,
          collectedBlocks, removedINodes, null, countDiffChange);
      if (isQuotaSet()) {
        ((INodeDirectoryWithQuota) this).addSpaceConsumed2Cache(
            -counts.get(Quota.NAMESPACE), -counts.get(Quota.DISKSPACE));
      }
      return counts;
    }
  }
  
  /**
   * Compare the metadata with another INodeDirectory
   */
  public boolean metadataEquals(INodeDirectory other) {
    return other != null && getNsQuota() == other.getNsQuota()
        && getDsQuota() == other.getDsQuota()
        && getUserName().equals(other.getUserName())
        && getGroupName().equals(other.getGroupName())
        && getFsPermission().equals(other.getFsPermission());
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
    out.print(", childrenSize=" + getChildrenList(snapshot).size());
    if (this instanceof INodeDirectoryWithQuota) {
      out.print(((INodeDirectoryWithQuota)this).quotaString());
    }
    if (this instanceof Snapshot.Root) {
      out.print(", snapshotId=" + snapshot.getId());
    }
    out.println();

    if (prefix.length() >= 2) {
      prefix.setLength(prefix.length() - 2);
      prefix.append("  ");
    }
    dumpTreeRecursively(out, prefix, new Iterable<SnapshotAndINode>() {
      final Iterator<INode> i = getChildrenList(snapshot).iterator();
      
      @Override
      public Iterator<SnapshotAndINode> iterator() {
        return new Iterator<SnapshotAndINode>() {
          @Override
          public boolean hasNext() {
            return i.hasNext();
          }

          @Override
          public SnapshotAndINode next() {
            return new SnapshotAndINode(snapshot, i.next());
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
      StringBuilder prefix, Iterable<SnapshotAndINode> subs) {
    if (subs != null) {
      for(final Iterator<SnapshotAndINode> i = subs.iterator(); i.hasNext();) {
        final SnapshotAndINode pair = i.next();
        prefix.append(i.hasNext()? DUMPTREE_EXCEPT_LAST_ITEM: DUMPTREE_LAST_ITEM);
        pair.inode.dumpTreeRecursively(out, prefix, pair.snapshot);
        prefix.setLength(prefix.length() - 2);
      }
    }
  }

  /** A pair of Snapshot and INode objects. */
  protected static class SnapshotAndINode {
    public final Snapshot snapshot;
    public final INode inode;

    public SnapshotAndINode(Snapshot snapshot, INode inode) {
      this.snapshot = snapshot;
      this.inode = inode;
    }

    public SnapshotAndINode(Snapshot snapshot) {
      this(snapshot, snapshot.getRoot());
    }
  }
}
