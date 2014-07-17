package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;

final class AzureFileSystemMetricsSystem {
  private static MetricsSystemImpl instance;
  private static int numFileSystems;
  
  public synchronized static void fileSystemStarted() {
    if (numFileSystems == 0) {
      instance = new MetricsSystemImpl();
      instance.init("azure-file-system");
    }
    numFileSystems++;
  }
  
  public synchronized static void fileSystemClosed() {
    if (numFileSystems == 1) {
      instance.publishMetricsNow();
      instance.stop();
      instance.shutdown();
      instance = null;
    }
    numFileSystems--;
  }

  public static void registerSource(String name, String desc,
      MetricsSource source) {
    //caller has to use unique name to register source
    instance.register(name, desc, source);
  }

  public static synchronized void unregisterSource(String name) {
    if (instance != null) {
      //publish metrics before unregister a metrics source
      instance.publishMetricsNow();
      instance.unregisterSource(name);
    }
  }
}
