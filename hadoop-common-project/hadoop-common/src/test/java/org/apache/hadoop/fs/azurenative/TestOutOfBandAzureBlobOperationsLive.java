package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.*;

import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.apache.hadoop.fs.azurenative.AzureMetricsTestUtil.*;
import static org.apache.hadoop.fs.azurenative.AzureFileSystemInstrumentation.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import org.hamcrest.*;

import com.microsoft.windowsazure.storage.blob.*;


import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;


public class TestOutOfBandAzureBlobOperationsLive {
  private FileSystem fs;
  private AzureBlobStorageTestAccount testAccount;

  @Before
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
    assumeNotNull(testAccount);
  }

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
      fs = null;
    }
  }


  // scenario for this particular test described at MONARCH-HADOOP-764
  // creating a file out-of-band would confuse mkdirs("<oobfilesUncleFolder>")
  //  eg oob creation of "user/<name>/testFolder/a/input/file"
  //     Then wasb creation of "user/<name>/testFolder/a/output" fails
  @Test
  public void outOfBandFolder_uncleMkdirs() throws Exception {

	  // NOTE: manual use of CloubBlockBlob targets working directory explicitly.
	  //       WASB driver methods prepend working directory implicitly.
	  String workingDir = "user/" + UserGroupInformation.getCurrentUser().getShortUserName() + "/";
	  
	  CloudBlockBlob blob = testAccount.getBlobReference(workingDir + "testFolder1/a/input/file"); 
	  BlobOutputStream s = blob.openOutputStream();
	  s.close();
	  assertTrue(fs.exists(new Path("testFolder1/a/input/file")));
	  
	  Path targetFolder = new Path("testFolder1/a/output"); 
	  assertTrue(fs.mkdirs(targetFolder));
  }
  
  //scenario for this particular test described at MONARCH-HADOOP-764
  @Test
  public void outOfBandFolder_parentDelete() throws Exception {
	  
	  //NOTE: manual use of CloubBlockBlob targets working directory explicitly.
	  //       WASB driver methods prepend working directory implicitly.
	  String workingDir = "user/" + UserGroupInformation.getCurrentUser().getShortUserName() + "/";
	  CloudBlockBlob blob = testAccount.getBlobReference(workingDir + "testFolder2/a/input/file"); 
	  BlobOutputStream s = blob.openOutputStream();
	  s.close();
	  assertTrue(fs.exists(new Path("testFolder2/a/input/file")));
	  
	  Path targetFolder = new Path("testFolder2/a/input");
	  assertTrue(fs.delete(targetFolder,true));
  }
  
  @Test
  public void outOfBandFolder_rootFileDelete() throws Exception {
	  
	  CloudBlockBlob blob = testAccount.getBlobReference("fileY"); 
	  BlobOutputStream s = blob.openOutputStream();
	  s.close();
	  assertTrue(fs.exists(new Path("/fileY")));
	  assertTrue(fs.delete(new Path("/fileY"),true));
  }
  
  @Test
  public void outOfBandFolder_firstLevelFolderDelete() throws Exception {
	  
	  CloudBlockBlob blob = testAccount.getBlobReference("folderW/file"); 
	  BlobOutputStream s = blob.openOutputStream();
	  s.close();
	  assertTrue(fs.exists(new Path("/folderW")));
	  assertTrue(fs.exists(new Path("/folderW/file")));
	  assertTrue(fs.delete(new Path("/folderW"),true));
  }
  
  //scenario for this particular test described at MONARCH-HADOOP-764
  @Test
  public void outOfBandFolder_siblingCreate() throws Exception {
	  
	  //NOTE: manual use of CloubBlockBlob targets working directory explicitly.
	  //       WASB driver methods prepend working directory implicitly.
	  String workingDir = "user/" + UserGroupInformation.getCurrentUser().getShortUserName() + "/";
	  CloudBlockBlob blob = testAccount.getBlobReference(workingDir + "testFolder3/a/input/file");
	  BlobOutputStream s = blob.openOutputStream();
	  s.close();
	  assertTrue(fs.exists(new Path("testFolder3/a/input/file")));
	  
	  Path targetFile = new Path("testFolder3/a/input/file2");
	  FSDataOutputStream s2 = fs.create(targetFile);
	  s2.close();
  }
  
  //scenario for this particular test described at MONARCH-HADOOP-764
  // creating a new file in the root folder
  @Test
  public void outOfBandFolder_create_rootDir() throws Exception {
	  Path targetFile = new Path("/newInRoot");
	  FSDataOutputStream s2 = fs.create(targetFile);
	  s2.close();
  }

  //scenario for this particular test described at MONARCH-HADOOP-764
  @Test
  public void outOfBandFolder_rename() throws Exception {
	  
	  //NOTE: manual use of CloubBlockBlob targets working directory explicitly.
	  //       WASB driver methods prepend working directory implicitly.
	  String workingDir = "user/" + UserGroupInformation.getCurrentUser().getShortUserName() + "/";
	  CloudBlockBlob blob = testAccount.getBlobReference(workingDir + "testFolder4/a/input/file");
	  BlobOutputStream s = blob.openOutputStream();
	  s.close();
	  
	  Path srcFilePath = new Path("testFolder4/a/input/file");
	  assertTrue(fs.exists(srcFilePath));
	  
	  Path destFilePath = new Path("testFolder4/a/input/file2");
	  fs.rename(srcFilePath, destFilePath);
  }
  
  //scenario for this particular test described at MONARCH-HADOOP-764
  @Test
  public void outOfBandFolder_rename_rootLevelFiles() throws Exception {
	  
	  //NOTE: manual use of CloubBlockBlob targets working directory explicitly.
	  //       WASB driver methods prepend working directory implicitly.
	  CloudBlockBlob blob = testAccount.getBlobReference("fileX");
	  BlobOutputStream s = blob.openOutputStream();
	  s.close();
	  
	  Path srcFilePath = new Path("/fileX");
	  assertTrue(fs.exists(srcFilePath));
	  
	  Path destFilePath = new Path("/fileXrename");
	  fs.rename(srcFilePath, destFilePath);
  } 
}
