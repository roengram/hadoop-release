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
package org.apache.hadoop.hdfs.protocol;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/**
 * Metadata about a snapshottable directory
 */
public class SnapshottableDirectoryStatus implements Writable {
  static {                                      // register a ctor
    WritableFactories.setFactory(SnapshottableDirectoryStatus.class,
        new WritableFactory() {
          public Writable newInstance() {
            return new SnapshottableDirectoryStatus();
          }
        });
  }
  
  /** Basic information of the snapshottable directory */
  private HdfsFileStatus dirStatus;
  
  /** Number of snapshots that have been taken*/
  private int snapshotNumber;
  
  /** Number of snapshots allowed. */
  private int snapshotQuota;
  
  /** Full path of the parent. */
  private byte[] parentFullPath;
  
  public SnapshottableDirectoryStatus() {
    this.dirStatus = new HdfsFileStatus();
    snapshotNumber = 0;
    snapshotQuota = 0;
    parentFullPath = null;
  }
  
  public SnapshottableDirectoryStatus(long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] localName,
      int snapshotNumber, int snapshotQuota, byte[] parentFullPath) {
    this.dirStatus = new HdfsFileStatus(0, true, 0, 0, modification_time,
        access_time, permission, owner, group, localName);
    this.snapshotNumber = snapshotNumber;
    this.snapshotQuota = snapshotQuota;
    this.parentFullPath = parentFullPath;
  }

  /**
   * @return Number of snapshots that have been taken for the directory
   */
  public int getSnapshotNumber() {
    return snapshotNumber;
  }

  /**
   * @return Number of snapshots allowed for the directory
   */
  public int getSnapshotQuota() {
    return snapshotQuota;
  }
  
  /**
   * @return Full path of the parent
   */
  public byte[] getParentFullPath() {
    return parentFullPath;
  }

  /**
   * @return The basic information of the directory
   */
  public HdfsFileStatus getDirStatus() {
    return dirStatus;
  }
  
  /**
   * @return Full path of the file
   */
  public Path getFullPath() {
    String parentFullPathStr = 
        (parentFullPath == null || parentFullPath.length == 0) ? "/"
        : DFSUtil.bytes2String(parentFullPath);
    return parentFullPathStr == null ? new Path(dirStatus.getLocalName())
        : new Path(parentFullPathStr, dirStatus.getLocalName());
  }
  
  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    this.dirStatus.write(out);
    out.writeInt(snapshotNumber);
    out.writeInt(snapshotQuota);
    out.writeInt(parentFullPath.length);
    out.write(parentFullPath);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    dirStatus.readFields(in);
    snapshotNumber = in.readInt();
    snapshotQuota = in.readInt();
    int numOfBytes = in.readInt();
    if (numOfBytes == 0) {
      parentFullPath = HdfsFileStatus.EMPTY_NAME;
    } else {
      parentFullPath = new byte[numOfBytes];
      in.readFully(parentFullPath);
    }
  }
}
