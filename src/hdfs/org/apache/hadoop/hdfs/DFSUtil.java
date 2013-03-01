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

package org.apache.hadoop.hdfs;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Comparator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.net.NodeBase;

public class DFSUtil {
  public static final byte[] EMPTY_BYTES = {};
  
  /**
   * Compare two byte arrays.
   * 
   * @return a negative integer, zero, or a positive integer 
   * as defined by {@link #compareTo(byte[])}.
   */
  public static int compareBytes(byte[] a1, byte[] a2) {
    if (a1 == a2)
      return 0;
    int len1 = (a1 == null ? 0 : a1.length);
    int len2 = (a2 == null ? 0 : a2.length);
    int n = Math.min(len1, len2);
    byte b1, b2;
    for (int i = 0; i < n; i++) {
      b1 = a1[i];
      b2 = a2[i];
      if (b1 != b2)
        return b1 - b2;
    }
    return len1 - len2;
  }
  
  /**
   * Whether the pathname is valid.  Currently prohibits relative paths, 
   * and names which contain a ":" or "/" 
   */
  public static boolean isValidName(String src) {
      
    // Path must be absolute.
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }
      
    // Check for ".." "." ":" "/"
    StringTokenizer tokens = new StringTokenizer(src, Path.SEPARATOR);
    while(tokens.hasMoreTokens()) {
      String element = tokens.nextToken();
      if (element.equals("..") || 
          element.equals(".")  ||
          (element.indexOf(":") >= 0)  ||
          (element.indexOf("/") >= 0)) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  public static String bytes2String(byte[] bytes) {
    return bytes2String(bytes, 0, bytes.length);
  }
  
  /**
   * Decode a specific range of bytes of the given byte array to a string
   * using UTF8.
   * 
   * @param bytes The bytes to be decoded into characters
   * @param offset The index of the first byte to decode
   * @param length The number of bytes to decode
   * @return The decoded string
   */
  public static String bytes2String(byte[] bytes, int offset, int length) {
    try {
      return new String(bytes, offset, length, "UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }
  
  /**
   * Given a list of path components returns a path as a UTF8 String
   */
  public static String byteArray2PathString(byte[][] pathComponents) {
    if (pathComponents.length == 0) {
      return "";
    } else if (pathComponents.length == 1
        && (pathComponents[0] == null || pathComponents[0].length == 0)) {
      return Path.SEPARATOR;
    }
    try {
      StringBuilder result = new StringBuilder();
      for (int i = 0; i < pathComponents.length; i++) {
        result.append(new String(pathComponents[i], "UTF-8"));
        if (i < pathComponents.length - 1) {
          result.append(Path.SEPARATOR_CHAR);
        }
      }
      return result.toString();
    } catch (UnsupportedEncodingException ex) {
      throw new AssertionError("UTF-8 encoding is not supported.");
    }
  }
  
  /**
   * Splits first len bytes in bytes to array of arrays of bytes
   * on byte separator
   * @param bytes the byte array to split
   * @param len the number of bytes to split
   * @param separator the delimiting byte
   */
  public static byte[][] bytes2byteArray(byte[] bytes,
                                         int len,
                                         byte separator) {
    assert len <= bytes.length;
    int splits = 0;
    if (len == 0) {
      return new byte[][]{null};
    }
    // Count the splits. Omit multiple separators and the last one
    for (int i = 0; i < len; i++) {
      if (bytes[i] == separator) {
        splits++;
      }
    }
    int last = len - 1;
    while (last > -1 && bytes[last--] == separator) {
      splits--;
    }
    if (splits == 0 && bytes[0] == separator) {
      return new byte[][]{null};
    }
    splits++;
    byte[][] result = new byte[splits][];
    int startIndex = 0;
    int nextIndex = 0;
    int index = 0;
    // Build the splits
    while (index < splits) {
      while (nextIndex < len && bytes[nextIndex] != separator) {
        nextIndex++;
      }
      result[index] = new byte[nextIndex - startIndex];
      System.arraycopy(bytes, startIndex, result[index], 0, nextIndex
              - startIndex);
      index++;
      startIndex = nextIndex + 1;
      nextIndex = startIndex;
    }
    return result;
  }
  
  /**
   * Given a list of path components returns a byte array
   */
  public static byte[] byteArray2bytes(byte[][] pathComponents) {
    if (pathComponents.length == 0) {
      return EMPTY_BYTES;
    } else if (pathComponents.length == 1
        && (pathComponents[0] == null || pathComponents[0].length == 0)) {
      return new byte[]{(byte) Path.SEPARATOR_CHAR};
    }
    int length = 0;
    for (int i = 0; i < pathComponents.length; i++) {
      length += pathComponents[i].length;
      if (i < pathComponents.length - 1) {
        length++; // for SEPARATOR
      }
    }
    byte[] path = new byte[length];
    int index = 0;
    for (int i = 0; i < pathComponents.length; i++) {
      System.arraycopy(pathComponents[i], 0, path, index,
          pathComponents[i].length);
      index += pathComponents[i].length;
      if (i < pathComponents.length - 1) {
        path[index] = (byte) Path.SEPARATOR_CHAR;
        index++;
      }
    }
    return path;
  }
  
  /** Convert an object representing a path to a string. */
  public static String path2String(final Object path) {
    return path == null? null
        : path instanceof String? (String)path
        : path instanceof byte[][]? byteArray2PathString((byte[][])path)
        : path.toString();
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    try {
      return str.getBytes("UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Convert a LocatedBlocks to BlockLocations[]
   * @param blocks a LocatedBlocks
   * @return an array of BlockLocations
   */
  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.locatedBlockCount();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] names = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        names[hCnt] = locations[hCnt].getName();
        NodeBase node = new NodeBase(names[hCnt], 
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] = new BlockLocation(names, hosts, racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize());
      idx++;
    }
    return blkLocations;
  }

  /** Create a URI from the scheme and address */
  public static URI createUri(String scheme, InetSocketAddress address) {
    try {
      return new URI(scheme, null, address.getHostName(), address.getPort(),
          null, null, null);
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException(ue);
    }
  }
  
  /**
   * Get DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION from configuration.
   * 
   * @param conf Configuration
   * @return Value of DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION
   * @throws IllegalArgumentException If the value is not positive
   */
  public static float getInvalidateWorkPctPerIteration(Configuration conf) {
    float blocksInvalidateWorkPct = conf.getFloat(
        DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION,
        DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION_DEFAULT);
    if (blocksInvalidateWorkPct <= 0.0f || blocksInvalidateWorkPct > 1.0f) {
      throw new IllegalArgumentException(
          DFSConfigKeys.DFS_NAMENODE_INVALIDATE_WORK_PCT_PER_ITERATION + " = '"
              + blocksInvalidateWorkPct + "' is invalid. "
              + "It should be a positive, non-zero float value "
              + "indicating a percentage.");
    }
    return blocksInvalidateWorkPct; 
  }
  
  /**
   * Get DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION 
   * from configuration.
   * 
   * @param conf Configuration
   * @return Value of DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION
   * @throws IllegalArgumentException If the value is not positive
   */
  public static int getReplWorkMultiplier(Configuration conf) {
    int blocksReplWorkMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION,
        DFSConfigKeys.
          DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION_DEFAULT);
    if (blocksReplWorkMultiplier <= 0) {
      throw new IllegalArgumentException(
          DFSConfigKeys.DFS_NAMENODE_REPLICATION_WORK_MULTIPLIER_PER_ITERATION
              + " = '" + blocksReplWorkMultiplier + "' is invalid. "
              + "It should be a positive, non-zero integer value.");
    }
    return blocksReplWorkMultiplier;
  }
   
  /**
   * Comparator for sorting DataNodeInfo[] based on stale states. Stale nodes
   * are moved to the end of the array on sorting with this comparator.
   */
  public static class StaleComparator implements Comparator<DatanodeInfo> {
    private long staleInterval;

    /**
     * Constructor of StaleComparator
     * 
     * @param interval
     *          The time invertal for marking datanodes as stale is passed from
     *          outside, since the interval may be changed dynamically
     */
    public StaleComparator(long interval) {
      this.staleInterval = interval;
    }

    @Override
    public int compare(DatanodeInfo a, DatanodeInfo b) {
      // Stale nodes will be moved behind the normal nodes
      boolean aStale = a.isStale(staleInterval);
      boolean bStale = b.isStale(staleInterval);
      return aStale == bStale ? 0 : (aStale ? 1 : -1);
    }
  }   
}

