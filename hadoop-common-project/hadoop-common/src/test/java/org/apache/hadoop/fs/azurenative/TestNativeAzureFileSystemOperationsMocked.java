package org.apache.hadoop.fs.azurenative;

import static org.junit.Assume.assumeTrue;
import org.apache.hadoop.fs.*;

public class TestNativeAzureFileSystemOperationsMocked extends
  FSMainOperationsBaseTest {

  private static final String TEST_ROOT_DIR =
    "/tmp/TestNativeAzureFileSystemOperationsMocked";

  // TODO added during manual merge -- verify correctness
  public TestNativeAzureFileSystemOperationsMocked (){
    super(TEST_ROOT_DIR);
  }

  // TODO added during manual merge -- verify correctness
  @Override
  public void setUp() throws Exception {
    fSys = AzureBlobStorageTestAccount.createMock().getFileSystem();
  }

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

  @Override
  public String getTestRootDir() {
    return TEST_ROOT_DIR;
  }

  @Override
  public Path getTestRootPath(FileSystem fSys) {
    return fSys.makeQualified(new Path(TEST_ROOT_DIR));
  }

  @Override
  public Path getTestRootPath(FileSystem fSys, String pathString) {
    return fSys.makeQualified(new Path(TEST_ROOT_DIR, pathString));
  }

  @Override
  public Path getAbsoluteTestRootPath(FileSystem fSys) {
    Path testRootPath = new Path(TEST_ROOT_DIR);
    if (testRootPath.isAbsolute()) {
      return testRootPath;
    } else {
      return new Path(fSys.getWorkingDirectory(), TEST_ROOT_DIR);
    }
  }

//  Removed during manual merge
// TODO delete if not needed
//
//  @Override
//  protected FileSystem createFileSystem() throws Exception {
//	throw new UnsupportedOperationException();
//  }
}
