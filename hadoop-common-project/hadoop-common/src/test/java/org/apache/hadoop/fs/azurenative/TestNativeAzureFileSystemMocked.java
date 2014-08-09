package org.apache.hadoop.fs.azurenative;

import java.io.IOException;

import org.junit.Ignore;
import org.junit.Test;

public class TestNativeAzureFileSystemMocked
    extends NativeAzureFileSystemBaseTest {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.createMock();
  }

  // Ignore the following tests because taking a lease requires a real
  // (not mock) file system store. These tests don't work on the mock.
  @Override
  @Ignore
  public void testLeaseAsDistributedLock() {
  }

  @Override
  @Ignore
  public void testSelfRenewingLease() {
  }

  @Override
  @Ignore
  public void testRedoFolderRenameAll() {
  }

  @Override
  @Ignore
  public void testCreateNonRecursive() {
  }

  @Override
  @Ignore
  public void testSelfRenewingLeaseFileDelete() {
  }

  @Override
  @Ignore
  public void testRenameRedoFolderAlreadyDone() throws IOException{
  }
}
