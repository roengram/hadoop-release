package org.apache.hadoop.fs.azurenative;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.RawLocalFileSystem;

public class Wasb extends DelegateToFileSystem {

  Wasb(final URI theUri, final Configuration conf) throws IOException,
      URISyntaxException {
    super(theUri, new NativeAzureFileSystem(), conf, "wasb", false);
  }

  @Override
  public int getUriDefaultPort() {
    return -1;
  }
}
