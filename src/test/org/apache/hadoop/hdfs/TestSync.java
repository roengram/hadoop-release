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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.DFSOutputStream;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.log4j.Level;
import org.junit.Test;

/**
 * Class contains a set of tests to verify the correctness of
 * {@link FSDataOutputStream#sync()} method
 */
public class TestSync {
  {
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
  }

  static final int BLOCK_SIZE = 1024;
  static final int NUM_BLOCKS = 10;
  static final int FILE_SIZE = NUM_BLOCKS * BLOCK_SIZE + 1;
  static long seed = 0;
  
  private final String fName = "hflushtest.dat";
  private static final String BYTES_PER_CHECKSUM = "io.bytes.per.checksum";
  
  /**
   * Test sync (with updating block length in NameNode) while no data is
   * actually written yet
   */
  @Test
  public void syncUpdateLength_0() throws IOException {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);
    DistributedFileSystem fileSystem = (DistributedFileSystem) cluster
        .getFileSystem();
    
    try {
      Path path = new Path(fName);
      FSDataOutputStream stm = fileSystem.create(path, true, 4096, (short) 2,
          BLOCK_SIZE);
      System.out.println("Created file " + path.toString());
      // sync with length updating
      ((DFSOutputStream) stm.getWrappedStream()).sync(true);
      long currentFileLength = fileSystem.getFileStatus(path).getLen();
      assertEquals(0L, currentFileLength);
      stm.close();
    } finally {
      fileSystem.close();
      cluster.shutdown();
    }
  }
  
  /**
   * Test sync while requiring updating the length in NameNode.
   */
  @Test
  public void syncUpdateLength_1() throws IOException {
    doTheJob(new Configuration(), fName, BLOCK_SIZE, (short) 2, true);
  }

  /**
   * Test sync while requiring updating the length in NameNode. It writes a file
   * with a custom block size so the writes will be happening across block
   * boundaries.
   */
  @Test
  public void syncUpdateLength_2() throws IOException {
    Configuration conf = new Configuration();
    int customPerChecksumSize = 512;
    int customBlockSize = customPerChecksumSize * 3;
    // Modify defaul filesystem settings
    conf.setInt(BYTES_PER_CHECKSUM, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);

    doTheJob(conf, fName, customBlockSize, (short) 2, true);
  }
  
  /**
   * Test sync while requiring updating the length in NameNode. It writes a file
   * with a custom block size so the writes will be happening across block and
   * checksum boundaries.
   */
  @Test
  public void syncUpdateLength_3() throws IOException {
    Configuration conf = new Configuration();
    int customPerChecksumSize = 400;
    int customBlockSize = customPerChecksumSize * 3;
    // Modify defaul filesystem settings
    conf.setInt(BYTES_PER_CHECKSUM, customPerChecksumSize);
    conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, customBlockSize);

    doTheJob(conf, fName, customBlockSize, (short) 2, true);
  }
  
  /**
   * The method starts new cluster with defined Configuration; creates a file
   * with specified block_size and writes 10 equal sections in it; it also calls
   * sync after each write and throws an IOException in case of an error.
   * 
   * @param conf cluster configuration
   * @param fileName of the file to be created and processed as required
   * @param block_size value to be used for the file's creation
   * @param replicas is the number of replicas
   * @param updateLength Whether or not to update the length in NameNode while 
   *                     calling sync
   * @throws IOException in case of any errors
   */
  public static void doTheJob(Configuration conf, final String fileName,
      long block_size, short replicas, boolean updateLength) throws IOException {
    byte[] fileContent;
    final int SECTIONS = 10;

    fileContent = AppendTestUtil.randomBytes(seed, FILE_SIZE);
    MiniDFSCluster cluster = new MiniDFSCluster(conf, replicas, true, null);
    // Make sure we work with DFS in order to utilize all its functionality
    DistributedFileSystem fileSystem = (DistributedFileSystem) cluster
        .getFileSystem();
    
    try {
      Path path = new Path(fileName);
      FSDataOutputStream stm = fileSystem.create(path, false, 4096, replicas,
          block_size);
      System.out.println("Created file " + fileName);

      int tenth = FILE_SIZE / SECTIONS;
      int rounding = FILE_SIZE - tenth * SECTIONS;
      for (int i = 0; i < SECTIONS; i++) {
        System.out.println("Writing " + (tenth * i) + " to "
            + (tenth * (i + 1)) + " section to file " + fileName);
        stm.write(fileContent, tenth * i, tenth);
        
        // Wait while sync pushes all packets through built pipeline
        ((DFSOutputStream)stm.getWrappedStream()).sync(updateLength);
        
        // Check file length if updateLength is required
        if (updateLength) {
          long currentFileLength = fileSystem.getFileStatus(path).getLen();
          assertEquals("File size doesn't match sync", tenth * (i + 1),
              currentFileLength);
        }
      }
      System.out.println("Writing " + (tenth * SECTIONS) + " to "
          + (tenth * SECTIONS + rounding) + " section to file " + fileName);
      stm.write(fileContent, tenth * SECTIONS, rounding);
      stm.close();

      assertEquals("File size doesn't match ", FILE_SIZE, fileSystem
          .getFileStatus(path).getLen());
    } finally {
      fileSystem.close();
      cluster.shutdown();
    }
  }
}
