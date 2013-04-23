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

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.BlocksMap.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectorySnapshottable;
import org.apache.hadoop.hdfs.server.namenode.snapshot.INodeDirectoryWithSnapshot;
import org.apache.hadoop.hdfs.server.namenode.snapshot.SnapshotFSImageFormat;
import org.apache.hadoop.io.UTF8;

/**
 * Static utility functions for serializing various pieces of data in the correct
 * format for the FSImage file.
 *
 * Some members are currently public for the benefit of the Offline Image Viewer
 * which is located outside of this package. These members should be made
 * package-protected when the OIV is refactored.
 */
@SuppressWarnings("deprecation")
public class FSImageSerialization {
  // Static-only class
  private FSImageSerialization() {}
  
  static private final UTF8 U_STR = new UTF8();
  static private final FsPermission FILE_PERM = new FsPermission((short) 0);

  public static String readString(DataInputStream in) throws IOException {
    U_STR.readFields(in);
    return U_STR.toString();
  }

  static String readString_EmptyAsNull(DataInputStream in) throws IOException {
    final String s = readString(in);
    return s.isEmpty() ? null : s;
  }

  static byte[] readBytes(DataInputStream in) throws IOException {
    U_STR.readFields(in);
    int len = U_STR.getLength();
    byte[] bytes = new byte[len];
    System.arraycopy(U_STR.getBytes(), 0, bytes, 0, len);
    return bytes;
  }
  
  /**
   * Reading the path from the image and converting it to byte[][] directly
   * this saves us an array copy and conversions to and from String
   * @param in
   * @return the array each element of which is a byte[] representation 
   *            of a path component
   */
  public static byte[][] readPathComponents(DataInputStream in)
      throws IOException {
    U_STR.readFields(in);
    return DFSUtil.bytes2byteArray(U_STR.getBytes(),
        U_STR.getLength(), (byte) Path.SEPARATOR_CHAR);
  }

  // Helper function that reads in an INodeUnderConstruction
  // from the input stream
  //
  static INodeFileUnderConstruction readINodeUnderConstruction(
      DataInputStream in, boolean toReadInodeId) throws IOException {
    final long id = toReadInodeId ? in.readLong() : FSNamesystem
        .getFSNamesystem().allocateNewInodeId();
    byte[] name = FSImageSerialization.readBytes(in);
    short blockReplication = in.readShort();
    long modificationTime = in.readLong();
    long preferredBlockSize = in.readLong();
    
    int numBlocks = in.readInt();
    BlockInfo[] blocks = new BlockInfo[numBlocks];
    Block blk = new Block();
    for (int i = 0; i < numBlocks; i++) {
      blk.readFields(in);
      blocks[i] = new BlockInfo(blk, blockReplication);
    }
    PermissionStatus perm = PermissionStatus.read(in);
    String clientName = FSImageSerialization.readString(in);
    String clientMachine = FSImageSerialization.readString(in);

    // the number of locations should be 0, and it is useless
    in.readInt();

    return new INodeFileUnderConstruction(id, name, blockReplication,
        modificationTime, preferredBlockSize, blocks, perm, clientName,
        clientMachine, null);
  }
  
  public static void writeString(String str, DataOutputStream out)
      throws IOException {
    U_STR.set(str);
    U_STR.write(out);
  }
  
  private static void writePermissionStatus(INodeWithAdditionalFields inode,
      DataOutput out) throws IOException {
    final FsPermission p = FILE_PERM;
    p.fromShort(inode.getFsPermissionShort());
    PermissionStatus.write(out, inode.getUserName(), inode.getGroupName(), p);
  }
  
  private static void writeLocalName(INode node, DataOutput out)
      throws IOException {
    final byte[] name = node.getLocalNameBytes();
    out.writeShort(name.length);
    out.write(name);
  }
   
  /**
   * Save one inode's attributes to the image.
   */
  public static void saveINode2Image(INode node, DataOutputStream out,
      boolean writeUnderConstruction)
      throws IOException {
    if (node.isDirectory()) {
      writeINodeDirectory(node.asDirectory(), out);
    } else {
      writeINodeFile(node.asFile(), out, writeUnderConstruction);
    }
  }
  
  public static void writeINodeFile(INodeFile file, DataOutputStream out,
      boolean writeUnderConstruction) throws IOException {
    out.writeLong(file.getId());
    writeLocalName(file, out);
    out.writeShort(file.getFileReplication());
    out.writeLong(file.getModificationTime());
    out.writeLong(file.getAccessTime());
    out.writeLong(file.getPreferredBlockSize());

    writeBlocks(file.getBlocks(), out);
    SnapshotFSImageFormat.saveFileDiffList(file, out);

    if (writeUnderConstruction) {
      if (file.isUnderConstruction()) {
        out.writeBoolean(true);
        final INodeFileUnderConstruction uc = (INodeFileUnderConstruction)file;
        writeString(uc.getClientName(), out);
        writeString(uc.getClientMachine(), out);
      } else {
        out.writeBoolean(false);
      }
    }

    writePermissionStatus(file, out);
  }

  public static void writeINodeDirectory(INodeDirectory node, DataOutput out)
      throws IOException {
    out.writeLong(node.getId());
    writeLocalName(node, out);
    out.writeShort(0);  // replication
    out.writeLong(node.getModificationTime());
    out.writeLong(0);   // access time
    out.writeLong(0);   // preferred block size
    out.writeInt(-1);   // # of blocks

    out.writeLong(node.getNsQuota());
    out.writeLong(node.getDsQuota());
    if (node instanceof INodeDirectorySnapshottable) {
      out.writeBoolean(true);
    } else {
      out.writeBoolean(false);
      out.writeBoolean(node instanceof INodeDirectoryWithSnapshot);
    }
    
    writePermissionStatus(node, out);
  }
  
  private static void writeBlocks(final Block[] blocks,
      final DataOutputStream out) throws IOException {
    if (blocks == null) {
      out.writeInt(0);
    } else {
      out.writeInt(blocks.length);
      for (Block blk : blocks) {
        blk.write(out);
      }
    }
  }
  
  // Helper function that writes an INodeUnderConstruction
  // into the input stream
  //
  static void writeINodeUnderConstruction(DataOutputStream out,
                                           INodeFileUnderConstruction cons,
                                           String path) 
                                           throws IOException {
    out.writeLong(cons.getId());
    writeString(path, out);
    out.writeShort(cons.getFileReplication());
    out.writeLong(cons.getModificationTime());
    out.writeLong(cons.getPreferredBlockSize());

    writeBlocks(cons.getBlocks(), out);
    cons.getPermissionStatus().write(out);

    writeString(cons.getClientName(), out);
    writeString(cons.getClientMachine(), out);

    out.writeInt(0); //  do not store locations of last block
  }
}
