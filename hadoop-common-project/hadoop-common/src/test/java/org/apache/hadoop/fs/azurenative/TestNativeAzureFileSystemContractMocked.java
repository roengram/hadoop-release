package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.fs.FileSystemContractBaseTest;

public class TestNativeAzureFileSystemContractMocked extends
    FileSystemContractBaseTest {

  @Override
  protected void setUp() throws Exception {
    fs = AzureBlobStorageTestAccount.createMock().getFileSystem();
  }

}
