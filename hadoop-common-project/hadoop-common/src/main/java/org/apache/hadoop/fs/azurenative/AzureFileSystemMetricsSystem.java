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
    if (instance != null) {
      instance.publishMetricsNow();
    }
    if (numFileSystems == 1) {
      instance.stop();
      instance.shutdown();
      instance = null;
    }
    numFileSystems--;
  }
  
  public static void registerSource(String name, String desc,
      MetricsSource source) {
    // Register the source with the name appended with -WasbSystem
    // so that the name is globally unique.
    instance.register(name + "-WasbSystem", desc, source);
  }
}
