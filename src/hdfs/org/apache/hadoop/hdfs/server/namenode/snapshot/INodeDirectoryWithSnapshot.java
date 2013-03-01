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

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hdfs.server.namenode.FSImageSerialization;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.snapshot.diff.Diff;
import org.apache.hadoop.hdfs.server.namenode.snapshot.diff.Diff.Container;
import org.apache.hadoop.hdfs.server.namenode.snapshot.diff.Diff.UndoInfo;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

/**
 * The directory with snapshots. It maintains a list of snapshot diffs for
 * storing snapshot data. When there are modifications to the directory, the old
 * data is stored in the latest snapshot, if there is any.
 */
public class INodeDirectoryWithSnapshot extends INodeDirectoryWithQuota {
  /**
   * The difference between the current state and a previous snapshot
   * of the children list of an INodeDirectory.
   */
  static class ChildrenDiff extends Diff<byte[], INode> {
    ChildrenDiff() {}
    
    private ChildrenDiff(final List<INode> created, final List<INode> deleted) {
      super(created, deleted);
    }
    
    private final INode setCreatedChild(final int c, final INode newChild) {
      return getCreatedList().set(c, newChild);
    }
    
    /** Serialize {@link #created} */
    private void writeCreated(DataOutputStream out) throws IOException {
      final List<INode> created = getCreatedList();
      out.writeInt(created.size());
      for (INode node : created) {
        // For INode in created list, we only need to record its local name 
        byte[] name = node.getLocalNameBytes();
        out.writeShort(name.length);
        out.write(name);
      }
    }
    
    /** Serialize {@link #deleted} */
    private void writeDeleted(DataOutputStream out) throws IOException {
      final List<INode> deleted = getDeletedList();
      out.writeInt(deleted.size());
      for (INode node : deleted) {
        FSImageSerialization.saveINode2Image(node, out, true);
      }
    }
    
    /** Serialize to out */
    private void write(DataOutputStream out) throws IOException {
      writeCreated(out);
      writeDeleted(out);    
    }

    /** @return The list of INodeDirectory contained in the deleted list */
    private List<INodeDirectory> getDirsInDeleted() {
      List<INodeDirectory> dirList = new ArrayList<INodeDirectory>();
      for (INode node : getDeletedList()) {
        if (node.isDirectory()) {
          dirList.add((INodeDirectory) node);
        }
      }
      return dirList;
    }
  }

  /**
   * The difference of an {@link INodeDirectory} between two snapshots.
   */
  static class DirectoryDiff extends AbstractINodeDiff<INodeDirectory, DirectoryDiff> {
    /** The size of the children list at snapshot creation time. */
    private final int childrenSize;
    /** The children list diff. */
    private final ChildrenDiff diff;

    private DirectoryDiff(Snapshot snapshot, INodeDirectory dir) {
      super(snapshot, null, null);

      this.childrenSize = dir.getChildrenList(null).size();
      this.diff = new ChildrenDiff();
    }

    /** Constructor used by FSImage loading */
    DirectoryDiff(Snapshot snapshot, INodeDirectory snapshotINode,
        DirectoryDiff posteriorDiff, int childrenSize,
        List<INode> createdList, List<INode> deletedList) {
      super(snapshot, snapshotINode, posteriorDiff);
      this.childrenSize = childrenSize;
      this.diff = new ChildrenDiff(createdList, deletedList);
    }
    
    ChildrenDiff getChildrenDiff() {
      return diff;
    }
    
    /** Is the inode the root of the snapshot? */
    boolean isSnapshotRoot() {
      return snapshotINode == snapshot.getRoot();
    }

    @Override
    void combinePosteriorAndCollectBlocks(final INodeDirectory currentDir,
        final DirectoryDiff posterior, final BlocksMapUpdateInfo collectedBlocks) {
      diff.combinePosterior(posterior.diff, new Diff.Processor<INode>() {
        /** Collect blocks for deleted files. */
        @Override
        public void process(INode inode) {
          if (inode != null) {
            inode.destroySubtreeAndCollectBlocks(posterior.snapshot,
                collectedBlocks);
          }
        }
      });
    }

    /**
     * @return The children list of a directory in a snapshot.
     *         Since the snapshot is read-only, the logical view of the list is
     *         never changed although the internal data structure may mutate.
     */
    ReadOnlyList<INode> getChildrenList(final INodeDirectory currentDir) {
      return new ReadOnlyList<INode>() {
        private List<INode> children = null;

        private List<INode> initChildren() {
          if (children == null) {
            final ChildrenDiff combined = new ChildrenDiff();
            for(DirectoryDiff d = DirectoryDiff.this; d != null; d = d.getPosterior()) {
              combined.combinePosterior(d.diff, null);
            }
            children = combined.apply2Current(ReadOnlyList.Util.asList(
                currentDir.getChildrenList(null)));
          }
          return children;
        }

        @Override
        public Iterator<INode> iterator() {
          return initChildren().iterator();
        }
    
        @Override
        public boolean isEmpty() {
          return childrenSize == 0;
        }
    
        @Override
        public int size() {
          return childrenSize;
        }
    
        @Override
        public INode get(int i) {
          return initChildren().get(i);
        }
      };
    }

    /** @return the child with the given name. */
    INode getChild(byte[] name, boolean checkPosterior, INodeDirectory currentDir) {
      for(DirectoryDiff d = this; ; d = d.getPosterior()) {
        final Container<INode> returned = d.diff.accessPrevious(name);
        if (returned != null) {
          // the diff is able to determine the inode
          return returned.getElement(); 
        } else if (!checkPosterior) {
          // Since checkPosterior is false, return null, i.e. not found.   
          return null;
        } else if (d.getPosterior() == null) {
          // no more posterior diff, get from current inode.
          return currentDir.getChild(name, null);
        }
      }
    }
    
    @Override
    public String toString() {
      return super.toString() + " childrenSize=" + childrenSize + ", " + diff;
    }
    
    @Override
    void write(DataOutputStream out) throws IOException {
      writeSnapshotPath(out);
      out.writeInt(childrenSize);

      // write snapshotINode
      if (isSnapshotRoot()) {
        out.writeBoolean(true);
      } else {
        out.writeBoolean(false);
        if (snapshotINode != null) {
          out.writeBoolean(true);
          FSImageSerialization.writeINodeDirectory(snapshotINode, out);
        } else {
          out.writeBoolean(false);
        }
      }
      // Write diff. Node need to write poseriorDiff, since diffs is a list.
      diff.write(out);
    }

    ChildrenDiff getDiff() {
      return diff;
    }
  }
  
  static class DirectoryDiffFactory
    extends AbstractINodeDiff.Factory<INodeDirectory, DirectoryDiff> {
    static final DirectoryDiffFactory INSTANCE = new DirectoryDiffFactory();

    @Override
    DirectoryDiff createDiff(Snapshot snapshot, INodeDirectory currentDir) {
      return new DirectoryDiff(snapshot, currentDir);
    }

    @Override
    INodeDirectory createSnapshotCopy(INodeDirectory currentDir) {
      final INodeDirectory copy = currentDir instanceof INodeDirectoryWithQuota?
          new INodeDirectoryWithQuota(currentDir, false,
              currentDir.getNsQuota(), currentDir.getDsQuota())
      : new INodeDirectory(currentDir, false);
      copy.setChildren(null);
      return copy;
    }
  }

  /** A list of directory diffs. */
  static class DirectoryDiffList 
      extends AbstractINodeDiffList<INodeDirectory, DirectoryDiff> {
    DirectoryDiffList() {
      setFactory(DirectoryDiffFactory.INSTANCE);
    }
  }

  /** Diff list sorted by snapshot IDs, i.e. in chronological order. */
  private final DirectoryDiffList diffs;

  public INodeDirectoryWithSnapshot(INodeDirectory that) {
    this(that, true, that instanceof INodeDirectoryWithSnapshot?
        ((INodeDirectoryWithSnapshot)that).getDiffs(): null);
  }

  INodeDirectoryWithSnapshot(INodeDirectory that, boolean adopt,
      DirectoryDiffList diffs) {
    super(that, adopt, that.getNsQuota(), that.getDsQuota());
    this.diffs = diffs != null? diffs: new DirectoryDiffList();
  }

  /** @return the last snapshot. */
  public Snapshot getLastSnapshot() {
    return diffs.getLastSnapshot();
  }

  /** @return the snapshot diff list. */
  DirectoryDiffList getDiffs() {
    return diffs;
  }
  
  @Override
  public INodeDirectory getSnapshotINode(Snapshot snapshot) {
    return diffs.getSnapshotINode(snapshot, this);
  }

  @Override
  public INodeDirectoryWithSnapshot recordModification(final Snapshot latest) {
    return isInLatestSnapshot(latest)?
        saveSelf2Snapshot(latest, null): this;
  }

  /** Save the snapshot copy to the latest snapshot. */
  public INodeDirectoryWithSnapshot saveSelf2Snapshot(
      final Snapshot latest, final INodeDirectory snapshotCopy) {
    diffs.saveSelf2Snapshot(latest, this, snapshotCopy);
    return this;
  }

  @Override
  public INode saveChild2Snapshot(final INode child, final Snapshot latest,
      final INode snapshotCopy) {
    if (child.isDirectory()) {
      throw new IllegalStateException("child is a directory, child=" + child);
    }
    if (latest == null) {
      return child;
    }

    final DirectoryDiff diff = diffs.checkAndAddLatestSnapshotDiff(latest, this);
    if (diff.getChild(child.getLocalNameBytes(), false, this) != null) {
      // it was already saved in the latest snapshot earlier.  
      return child;
    }

    diff.diff.modify(snapshotCopy, child);
    return child;
  }

  @Override
  public boolean addChild(INode inode, boolean inheritPermission,
      Snapshot latest) {
    ChildrenDiff diff = null;
    Integer undoInfo = null;
    if (latest != null) {
      diff = diffs.checkAndAddLatestSnapshotDiff(latest, this).diff;
      undoInfo = diff.create(inode);
    }
    final boolean added = super.addChild(inode, inheritPermission, null);
    if (!added && undoInfo != null) {
      diff.undoCreate(inode, undoInfo);
    }
    return added; 
  }

  @Override
  public INode removeChild(INode child, Snapshot latest) {
    ChildrenDiff diff = null;
    UndoInfo<INode> undoInfo = null;
    if (latest != null) {
      diff = diffs.checkAndAddLatestSnapshotDiff(latest, this).diff;
      undoInfo = diff.delete(child);
    }
    final INode removed = super.removeChild(child, null);
    if (removed == null && undoInfo != null) {
      diff.undoDelete(child, undoInfo);
    }
    if (undoInfo != null) {
      if (removed == null) {
        //remove failed, undo
        diff.undoDelete(child, undoInfo);
      }
    }
    return removed;
  }

  @Override
  public void replaceChild(final INode oldChild, final INode newChild) {
    super.replaceChild(oldChild, newChild);

    // replace the created child, if there is any.
    final byte[] name = oldChild.getLocalNameBytes();
    final List<DirectoryDiff> diffList = diffs.asList();
    for(int i = diffList.size() - 1; i >= 0; i--) {
      final ChildrenDiff diff = diffList.get(i).diff;
      final int c = diff.searchCreatedIndex(name);
      if (c >= 0) {
        final INode removed = diff.setCreatedChild(c, newChild);
        if (removed != oldChild) {
          throw new IllegalStateException("removed != oldChild. removed = "
              + removed.toDetailString() + ". oldChild = "
              + oldChild.toDetailString());
        }
        return;
      }
    }
  }

  @Override
  public ReadOnlyList<INode> getChildrenList(Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getChildrenList(this): super.getChildrenList(null);
  }

  @Override
  public INode getChild(byte[] name, Snapshot snapshot) {
    final DirectoryDiff diff = diffs.getDiff(snapshot);
    return diff != null? diff.getChild(name, true, this): super.getChild(name, null);
  }

  @Override
  public String toDetailString() {
    return super.toDetailString() + ", " + diffs;
  }
  
  /**
   * Get all the directories that are stored in some snapshot but not in the
   * current children list. These directories are equivalent to the directories
   * stored in the deletes lists.
   * 
   * @param snapshotDirMap A snapshot-to-directory-list map for returning.
   * @return The number of directories returned.
   */
  public int getSnapshotDirectory(
      Map<Snapshot, List<INodeDirectory>> snapshotDirMap) {
    int dirNum = 0;
    for (DirectoryDiff sdiff : diffs) {
      List<INodeDirectory> list = sdiff.getChildrenDiff().getDirsInDeleted();
      if (list.size() > 0) {
        snapshotDirMap.put(sdiff.snapshot, list);
        dirNum += list.size();
      }
    }
    return dirNum;
  }
  
  @Override
  public int destroySubtreeAndCollectBlocks(final Snapshot snapshot,
      final BlocksMapUpdateInfo collectedBlocks) {
    int n = destroySubtreeAndCollectBlocksRecursively(snapshot, collectedBlocks);
    if (snapshot != null) {
      final DirectoryDiff removed = getDiffs().deleteSnapshotDiff(snapshot,
          this, collectedBlocks);
      if (removed != null) {
        n++; //count this dir only if a snapshot diff is removed.
      }
    }
    return n;
  }
}