package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.KeyProviderException;

/**
 * Key provider that simply returns the storage account key from the
 * configuration as plaintext.
 */
public class SimpleKeyProvider implements KeyProvider {

  protected static final String KEY_ACCOUNT_KEY_PREFIX =
      "fs.azure.account.key.";

  @Override
  public String getStorageAccountKey(String accountName, Configuration conf)
      throws KeyProviderException {
    return conf.get(getStorageAccountKeyName(accountName));
  }

  protected String getStorageAccountKeyName(String accountName) {
    return KEY_ACCOUNT_KEY_PREFIX + accountName; 
  }
}
