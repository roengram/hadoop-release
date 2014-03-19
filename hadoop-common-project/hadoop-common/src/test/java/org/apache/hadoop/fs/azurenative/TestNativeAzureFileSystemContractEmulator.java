package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.fs.FileSystemContractBaseTest;

public class TestNativeAzureFileSystemContractEmulator extends
FileSystemContractBaseTest {
  private AzureBlobStorageTestAccount testAccount;

  @Override
  protected void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createForEmulator();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
      fs = null;
    }
  }

  @Override
  protected void runTest() throws Throwable {
    if (testAccount != null) {
      super.runTest();
    }
  }
}
