package org.apache.hadoop.fs.azurenative;

/*
 * Tests the Native Azure file system (WASB) against an actual blob store if
 * provided in the environment.
 */
public class TestNativeAzureFileSystemLive
    extends NativeAzureFileSystemBaseTest {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount()
      throws Exception {
    return AzureBlobStorageTestAccount.create();
  }
}
