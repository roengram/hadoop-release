package org.apache.hadoop.fs.azurenative;

public class TestNativeAzureFileSystemMocked
    extends NativeAzureFileSystemBaseTest {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.createMock();
  }
}
