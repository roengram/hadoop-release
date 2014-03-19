package org.apache.hadoop.fs.azurenative;

import static org.apache.hadoop.fs.azurenative.AzureMetricsTestUtil.*;
import static org.junit.Assert.*;

import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.metrics2.lib.*;

import org.junit.*;

public class TestBandwidthGaugeUpdater {
  @Test
  public void testSingleThreaded() throws Exception {
    AzureFileSystemInstrumentation instrumentation = createInstrumentation();
    BandwidthGaugeUpdater updater =
        new BandwidthGaugeUpdater(instrumentation, 1000, true);
    updater.triggerUpdate(true);
    assertEquals(0, getCurrentBytesWritten(instrumentation));
    updater.blockUploaded(new Date(), new Date(), 150);
    updater.triggerUpdate(true);
    assertEquals(150, getCurrentBytesWritten(instrumentation));
    updater.blockUploaded(new Date(new Date().getTime() - 10000),
        new Date(), 200);
    updater.triggerUpdate(true);
    long currentBytes = getCurrentBytesWritten(instrumentation);
    assertTrue(
        "We expect around (200/10 = 20) bytes written as the gauge value." +
        "Got " + currentBytes,
        currentBytes > 18 && currentBytes < 22);
    updater.close();
  }

  @Test
  public void testMultiThreaded() throws Exception {
    AzureFileSystemInstrumentation instrumentation = createInstrumentation();
    final BandwidthGaugeUpdater updater =
        new BandwidthGaugeUpdater(instrumentation, 1000, true);
    Thread[] threads = new Thread[10];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          updater.blockDownloaded(new Date(), new Date(), 10);
          updater.blockDownloaded(new Date(0), new Date(0), 10);
        }
      });
    }
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    updater.triggerUpdate(false);
    assertEquals(10 * threads.length, getCurrentBytesRead(instrumentation));
    updater.close();
  }

  private AzureFileSystemInstrumentation createInstrumentation() {
    AzureFileSystemInstrumentation instrumentation =
        new AzureFileSystemInstrumentation(new Configuration());
    // Need to let the Metrics2 system build up the source so that
    // the fields are properly initialized.
    MetricsAnnotations.makeSource(instrumentation);
    return instrumentation;
  }
}
