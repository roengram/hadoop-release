package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.KeyProviderException;
import org.apache.hadoop.util.Shell;

import org.junit.*;

public class TestShellDecryptionKeyProvider {
  public static final Log LOG =
      LogFactory.getLog(TestShellDecryptionKeyProvider.class);
  private static File TEST_ROOT_DIR =
      new File(System.getProperty("test.build.data", "/tmp"),
          "TestShellDecryptionKeyProvider");

  @Test
  public void testScriptPathNotSpecified() throws Exception {
    if (!Shell.WINDOWS) {
      return;
    }
    ShellDecryptionKeyProvider provider = new ShellDecryptionKeyProvider();
    Configuration conf = new Configuration();
    String account = "testacct";
    String key = "key";

    conf.set(SimpleKeyProvider.KEY_ACCOUNT_KEY_PREFIX + account, key);
    try {
      provider.getStorageAccountKey(account, conf);
      Assert.fail(
        "fs.azure.shellkeyprovider.script is not specified, we should throw");
    } catch (KeyProviderException e) {
      LOG.info("Received an expected exception: " + e.getMessage());
    }
  }

  @Test
  public void testValidScript() throws Exception {
    if (!Shell.WINDOWS) {
      return;
    }
    String expectedResult = "decretedKey";

    // Create a simple script which echoes the given key plus the given
    // expected result (so that we validate both script input and output)
    File scriptFile = new File(TEST_ROOT_DIR, "testScript.cmd");
    FileUtils.writeStringToFile(scriptFile, "@echo %1 " + expectedResult);

    ShellDecryptionKeyProvider provider = new ShellDecryptionKeyProvider();
    Configuration conf = new Configuration();
    String account = "testacct";
    String key = "key1";
    conf.set(SimpleKeyProvider.KEY_ACCOUNT_KEY_PREFIX + account, key);
    conf.set(ShellDecryptionKeyProvider.KEY_ACCOUNT_SHELLKEYPROVIDER_SCRIPT,
        "cmd /c " + scriptFile.getAbsolutePath());

    String result = provider.getStorageAccountKey(account, conf);
    assertEquals(key + " " + expectedResult, result);
  }  
}
