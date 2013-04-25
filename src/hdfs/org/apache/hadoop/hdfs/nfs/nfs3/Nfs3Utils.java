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
package org.apache.hadoop.hdfs.nfs.nfs3;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ExtendedHdfsFileStatus;
import org.apache.hadoop.nfs.NfsTime;
import org.apache.hadoop.nfs.nfs3.FileHandle;
import org.apache.hadoop.nfs.nfs3.IdUserGroup;
import org.apache.hadoop.nfs.nfs3.Nfs3Constant;
import org.apache.hadoop.nfs.nfs3.Nfs3FileAttributes;
import org.apache.hadoop.nfs.nfs3.response.WccAttr;
import org.apache.hadoop.oncrpc.XDR;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

/**
 * Utility/helper methods related to NFS
 */
public class Nfs3Utils {
  public final static String INODEID_PATH_PREFIX = "/.reserved/.inodes/";

  public static String getFileIdPath(FileHandle handle) {
    return getFileIdPath(handle.getFileId());
  }

  public static String getFileIdPath(long fileId) {
    return INODEID_PATH_PREFIX + fileId;
  }

  public static ExtendedHdfsFileStatus getFileStatus(DFSClient client,
      String fileIdPath) throws IOException {
    return client.getExtendedFileInfo(fileIdPath);
  }

  public static Nfs3FileAttributes getNfs3FileAttrFromFileStatus(
      ExtendedHdfsFileStatus fs, IdUserGroup iug) {
    /**
     * Some 32bit Linux client has problem with 64bit fileId: it seems the 32bit
     * client takes only the lower 32bit of the fileId and treats it as signed
     * int. When the 32th bit is 1, the client considers it invalid.
     * 
     */
    return new Nfs3FileAttributes(fs.isDir(), (int) fs.getChildrenNum(), fs
        .getPermission().toShort(), iug.getUidAllowingUnknown(fs.getOwner()),
        iug.getGidAllowingUnknown(fs.getGroup()), fs.getLen(), 0 /* fsid */,
        fs.getFileId(), fs.getModificationTime(), fs.getAccessTime());
  }
  
  public static Nfs3FileAttributes getFileAttr(DFSClient client,
      String fileIdPath, IdUserGroup iug) throws IOException {
    ExtendedHdfsFileStatus fs = getFileStatus(client, fileIdPath);
    return fs == null ? null : getNfs3FileAttrFromFileStatus(fs, iug);
  }

  public static WccAttr getWccAttr(DFSClient client, String fileIdPath)
      throws IOException {
    ExtendedHdfsFileStatus fstat = getFileStatus(client, fileIdPath);
    if (fstat == null) {
      return null;
    }
    long size = fstat.isDir() ? Nfs3FileAttributes.getDirSize((int) fstat
        .getChildrenNum()) : fstat.getLen();
    return new WccAttr(size, new NfsTime(fstat.getModificationTime()),
        new NfsTime(fstat.getModificationTime()));
  }

  public static WccAttr getWccAttr(Nfs3FileAttributes attr) {
    return new WccAttr(attr.getSize(), attr.getMtime(), attr.getCtime());
  }

  /**
   * Send a write response to the netty network socket channel
   */
  public static void writeChannel(Channel channel, XDR out) {
    ChannelBuffer outBuf = XDR.writeRequest(out, true);
    channel.write(outBuf);
  }

  private static boolean isSet(int access, int bits) {
    return (access & bits) == bits;
  }

  public static int getAccessRights(int mode) {
    int rtn = 0;
    if (isSet(mode, Nfs3Constant.ACCESS_MODE_READ)) {
      rtn |= Nfs3Constant.ACCESS3_READ;
      // LOOKUP is only meaningful for dir, set anyway.
      rtn |= Nfs3Constant.ACCESS3_LOOKUP;
    }
    if (isSet(mode, Nfs3Constant.ACCESS_MODE_WRITE)) {
      rtn |= Nfs3Constant.ACCESS3_MODIFY;
      rtn |= Nfs3Constant.ACCESS3_EXTEND;
      // Never set delete bit since it's up to parent dir op permission
    }
    if (isSet(mode, Nfs3Constant.ACCESS_MODE_EXECUTE)) {
      rtn |= Nfs3Constant.ACCESS3_EXECUTE;
    }
    return rtn;
  }

  public static int getAccessRightsForUserGroup(int uid, int gid,
      Nfs3FileAttributes attr) {
    int mode = attr.getMode();
    int rtn = 0;
    if (uid == attr.getUid()) {
      rtn |= getAccessRights(mode);
    }
    mode = mode >> 3;
    if (gid == attr.getGid()) {
      rtn |= getAccessRights(mode);
    }
    mode = mode >> 3;
    if (uid == attr.getUid()) {
      rtn |= getAccessRights(mode);
    }
    return rtn;
  }

  public static long bytesToLong(byte[] data) {
    ByteBuffer buffer = ByteBuffer.allocate(8);
    for (int i = 0; i < 8; i++) {
      buffer.put(data[i]);
    }
    buffer.flip();// need flip
    return buffer.getLong();
  }

  public static byte[] longToByte(long v) {
    byte[] data = new byte[8];
    data[0] = (byte) (v >>> 56);
    data[1] = (byte) (v >>> 48);
    data[2] = (byte) (v >>> 40);
    data[3] = (byte) (v >>> 32);
    data[4] = (byte) (v >>> 24);
    data[5] = (byte) (v >>> 16);
    data[6] = (byte) (v >>> 8);
    data[7] = (byte) (v >>> 0);
    return data;
  }
}