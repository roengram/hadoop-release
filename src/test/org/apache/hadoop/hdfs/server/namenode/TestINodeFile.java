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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.junit.Test;

public class TestINodeFile {

  static final short BLOCKBITS = 48;
  static final long BLKSIZE_MAXVALUE = ~(0xffffL << BLOCKBITS);

  private String userName = "Test";
  private short replication;
  private long preferredBlockSize;

  /**
   * Test for the Replication value. Sets a value and checks if it was set
   * correct.
   */
  @Test
  public void testReplication () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null, 
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
    assertEquals("True has to be returned in this case", replication,
                 inf.getBlockReplication());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for Replication.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testReplicationBelowLowerBound ()
              throws IllegalArgumentException {
    replication = -1;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null,
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
  }

  /**
   * Test for the PreferredBlockSize value. Sets a value and checks if it was
   * set correct.
   */
  @Test
  public void testPreferredBlockSize () {
    replication = 3;
    preferredBlockSize = 128*1024*1024;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null,
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
    assertEquals("True has to be returned in this case", preferredBlockSize,
           inf.getPreferredBlockSize());
  }

  @Test
  public void testPreferredBlockSizeUpperBound () {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null, 
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
    assertEquals("True has to be returned in this case", BLKSIZE_MAXVALUE,
                 inf.getPreferredBlockSize());
  }

  /**
   * IllegalArgumentException is expected for setting below lower bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeBelowLowerBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = -1;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null, 
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
  } 

  /**
   * IllegalArgumentException is expected for setting above upper bound
   * for PreferredBlockSize.
   * @throws IllegalArgumentException as the result
   */
  @Test(expected=IllegalArgumentException.class)
  public void testPreferredBlockSizeAboveUpperBound ()
              throws IllegalArgumentException {
    replication = 3;
    preferredBlockSize = BLKSIZE_MAXVALUE+1;
    INodeFile inf = new INodeFile(new PermissionStatus(userName, null, 
                                  FsPermission.getDefault()), null, replication,
                                  0L, 0L, preferredBlockSize);
  }


  /**
   * Test for the static {@link INodeFile#valueOf(INode, String)}
   * and {@link INodeFileUnderConstruction#valueOf(INode, String)} methods.
   * @throws IOException 
   */
  @Test
  public void testValueOf () throws IOException {
    final String path = "/testValueOf";
    final PermissionStatus perm = new PermissionStatus(
        userName, null, FsPermission.getDefault());
    final short replication = 3;

    {//cast from null
      final INode from = null;

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("File does not exist"));
      }

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("File does not exist"));
      }

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch (FileNotFoundException e) {
        assertTrue(e.getMessage().contains("Directory does not exist"));
      }
    }

    {//cast from INodeFile
      final INode from = new INodeFile(
          perm, null, replication, 0L, 0L, preferredBlockSize);
      
      //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch(IOException ioe) {
        assertTrue(ioe.getMessage().contains("File is not under construction"));
      }

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(FileNotFoundException e) {
        assertTrue(e.getMessage().contains("Is not a directory"));
      }
    }

    {//cast from INodeFileUnderConstruction
      final INode from = new INodeFileUnderConstruction(
          perm, replication, 0L, 0L, "client", "machine", null);
      
      //cast to INodeFile, should success
      final INodeFile f = INodeFile.valueOf(from, path);
      assertTrue(f == from);

      //cast to INodeFileUnderConstruction, should success
      final INodeFileUnderConstruction u = INodeFileUnderConstruction.valueOf(
          from, path);
      assertTrue(u == from);

      //cast to INodeDirectory, should fail
      try {
        INodeDirectory.valueOf(from, path);
        fail();
      } catch(FileNotFoundException e) {
        assertTrue(e.getMessage().contains("Is not a directory"));
      }
    }

    {//cast from INodeDirectory
      final INode from = new INodeDirectory(perm, 0L);

      //cast to INodeFile, should fail
      try {
        INodeFile.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("Path is not a file"));
      }

      //cast to INodeFileUnderConstruction, should fail
      try {
        INodeFileUnderConstruction.valueOf(from, path);
        fail();
      } catch(FileNotFoundException fnfe) {
        assertTrue(fnfe.getMessage().contains("Path is not a file"));
      }

      //cast to INodeDirectory, should success
      final INodeDirectory d = INodeDirectory.valueOf(from, path);
      assertTrue(d == from);
    }
  }
}
