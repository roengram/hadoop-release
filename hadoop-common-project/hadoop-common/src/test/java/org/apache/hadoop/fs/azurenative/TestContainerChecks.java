package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.junit.*;

import com.microsoft.windowsazure.storage.blob.*;

import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azurenative.AzureBlobStorageTestAccount.CreateOptions;

/**
 * Tests that WASB creates containers only if needed.
 */
public class TestContainerChecks {
  private AzureBlobStorageTestAccount testAccount;

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
  }

  @Test
  public void testContainerExistAfterDoesNotExist() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
            EnumSet.noneOf(CreateOptions.class));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // Starting off with the container not there
    assertFalse(container.exists());

    // A list shouldn't create the container and will set file system store
    // state to DoesNotExist
    try {
      fs.listStatus(new Path("/"));
      assertTrue("Should've thrown.", false);
    } catch (FileNotFoundException ex) {
      assertTrue("Unexpected exception: " + ex,
          ex.getMessage().contains("does not exist."));
    }
    assertFalse(container.exists());

    // Create a container outside of the WASB FileSystem
    container.create();
    // Add a file to the container outside of the WASB FileSystem
    CloudBlockBlob blob = testAccount.getBlobReference("foo");
    BlobOutputStream outputStream = blob.openOutputStream();
    outputStream.write(new byte[10]);
    outputStream.close();

    // Make sure the file is visible
    assertTrue(fs.exists(new Path("/foo")));
    assertTrue(container.exists());
  }

  @Test
  public void testContainerCreateAfterDoesNotExist() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
            EnumSet.noneOf(CreateOptions.class));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // Starting off with the container not there
    assertFalse(container.exists());

    // A list shouldn't create the container and will set file system store
    // state to DoesNotExist
    try {
      assertNull(fs.listStatus(new Path("/")));
      assertTrue("Should've thrown.", false);
    } catch (FileNotFoundException ex) {
      assertTrue("Unexpected exception: " + ex,
          ex.getMessage().contains("does not exist."));
    }
    assertFalse(container.exists());

    // Create a container outside of the WASB FileSystem
    container.create();

    // Write should succeed
    assertTrue(fs.createNewFile(new Path("/foo")));
    assertTrue(container.exists());
  }

  @Test
  public void testContainerCreateOnWrite() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
            EnumSet.noneOf(CreateOptions.class));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // Starting off with the container not there
    assertFalse(container.exists());

    // A list shouldn't create the container.
    try {
      fs.listStatus(new Path("/"));
      assertTrue("Should've thrown.", false);
    } catch (FileNotFoundException ex) {
      assertTrue("Unexpected exception: " + ex,
          ex.getMessage().contains("does not exist."));
    }
    assertFalse(container.exists());
    
    // Neither should a read.
    try {
      fs.open(new Path("/foo"));
      assertFalse("Should've thrown.", true);
    } catch (FileNotFoundException ex) {
    }
    assertFalse(container.exists());

    // Neither should a rename
    assertFalse(fs.rename(new Path("/foo"), new Path("/bar")));
    assertFalse(container.exists());

    // But a write should.
    assertTrue(fs.createNewFile(new Path("/foo")));
    assertTrue(container.exists());
  }

  @Test
  public void testContainerChecksWithSas() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create("",
        EnumSet.of(CreateOptions.UseSas));
    assumeNotNull(testAccount);
    CloudBlobContainer container = testAccount.getRealContainer();
    FileSystem fs = testAccount.getFileSystem();

    // The container shouldn't be there
    assertFalse(container.exists());

    // A write should just fail
    try {
      fs.createNewFile(new Path("/foo"));
      assertFalse("Should've thrown.", true);
    } catch (AzureException ex) {
    }
    assertFalse(container.exists());
  }
}
