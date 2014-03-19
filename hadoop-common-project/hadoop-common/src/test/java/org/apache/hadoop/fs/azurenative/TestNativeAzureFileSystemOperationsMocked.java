package org.apache.hadoop.fs.azurenative;

import static org.junit.Assume.assumeTrue;
import org.apache.hadoop.fs.*;

public class TestNativeAzureFileSystemOperationsMocked extends
  FSMainOperationsBaseTest {

  @Override
  protected FileSystem createFileSystem() throws Exception {
    return AzureBlobStorageTestAccount.createMock().getFileSystem();
  }

  public void testListStatusThrowsExceptionForUnreadableDir()
      throws Exception {
    System.out.println(
        "Skipping testListStatusThrowsExceptionForUnreadableDir since WASB" +
        " doesn't honor directory permissions.");
    assumeTrue(!Path.WINDOWS);
  }
}
