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

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

/** Interface that represents the over the wire information for a file.
 */
public class ExtendedHdfsFileStatus extends HdfsFileStatus {
  static {                                      // register a ctor
    WritableFactories.setFactory
      (ExtendedHdfsFileStatus.class,
       new WritableFactory() {
         public Writable newInstance() { return new ExtendedHdfsFileStatus(); }
       });
  }
  private long fileId;
  
  // Used by dir, not including dot and dotdot. Always zero for a regular file.
  private int childrenNum; 

  /**
   * default constructor
   */
  public ExtendedHdfsFileStatus() {
    this(INodeId.GRANDFATHER_INODE_ID, 0, 0, false, 0, 0, 0, 0, null, null, null,
        null);
  }
  
  /**
   * Constructor
   * @param file id
   * @param childrenNum number of children
   * @param length the number of bytes the file has
   * @param isdir if the path is a directory
   * @param block_replication the replication factor
   * @param blocksize the block size
   * @param modification_time modification time
   * @param access_time access time
   * @param permission permission
   * @param owner the owner of the path
   * @param group the group of the path
   * @param path the local name in java UTF8 encoding the same as that in-memory
   */
  public ExtendedHdfsFileStatus(long fileId, int childrenNum, long length,
      boolean isdir, int block_replication, long blocksize,
      long modification_time, long access_time, FsPermission permission,
      String owner, String group, byte[] path) {
    super(length, isdir, block_replication, blocksize, modification_time,
        access_time, permission, owner, group, path);
    this.fileId = fileId;
    this.childrenNum = childrenNum;
  }

  final public long getFileId() {
    return fileId;
  }

  final public long getChildrenNum() {
    return childrenNum;
  }
  
  //////////////////////////////////////////////////
  // Writable
  //////////////////////////////////////////////////
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(fileId);
    out.writeInt(childrenNum);
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.fileId = in.readLong();
    this.childrenNum = in.readInt();
    super.readFields(in);
  }
}
