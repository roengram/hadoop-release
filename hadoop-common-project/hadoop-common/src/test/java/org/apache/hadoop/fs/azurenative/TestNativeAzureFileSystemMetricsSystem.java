package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;
import org.apache.hadoop.fs.*;
import org.junit.*;

/**
 * Tests that the WASB-specific metrics system is working correctly.
 */
public class TestNativeAzureFileSystemMetricsSystem {
  private static final String WASB_FILES_CREATED = "wasb_files_created";

  private static int getFilesCreated(AzureBlobStorageTestAccount testAccount) {
    return testAccount.getLatestMetricValue(WASB_FILES_CREATED, 0).intValue();    
  }

  /**
   * Tests that when we have multiple file systems created/destroyed 
   * metrics from each are published correctly.
   * @throws Exception 
   */
  @Test
  public void testMetricsAcrossFileSystems()
      throws Exception {
    AzureBlobStorageTestAccount a1, a2, a3;

    a1 = AzureBlobStorageTestAccount.createMock();
    assertEquals(0, getFilesCreated(a1));
    a2 = AzureBlobStorageTestAccount.createMock();
    assertEquals(0, getFilesCreated(a2));
    a1.getFileSystem().create(new Path("/foo")).close();
    a1.getFileSystem().create(new Path("/bar")).close();
    a2.getFileSystem().create(new Path("/baz")).close();
    assertEquals(0, getFilesCreated(a1));
    assertEquals(0, getFilesCreated(a2));
    a1.closeFileSystem(); // Causes the file system to close, which publishes metrics
    a2.closeFileSystem();
    assertEquals(2, getFilesCreated(a1));
    assertEquals(1, getFilesCreated(a2));
    a3 = AzureBlobStorageTestAccount.createMock();
    assertEquals(0, getFilesCreated(a3));
    a3.closeFileSystem();
    assertEquals(0, getFilesCreated(a3));
  }

  
  @Test
  public void testMetricsSourceNames() {
    String name1 = NativeAzureFileSystem.newMetricsSourceName();
    String name2 = NativeAzureFileSystem.newMetricsSourceName();
    assertTrue(name1.startsWith("AzureFileSystemMetrics"));
    assertTrue(name2.startsWith("AzureFileSystemMetrics"));
    assertTrue(!name1.equals(name2));
  }
}
