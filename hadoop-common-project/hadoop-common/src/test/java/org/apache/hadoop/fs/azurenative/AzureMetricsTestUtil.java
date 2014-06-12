package org.apache.hadoop.fs.azurenative;

import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.apache.hadoop.fs.azurenative.AzureFileSystemInstrumentation.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.mockito.ArgumentCaptor;
public final class AzureMetricsTestUtil {
  public static long getLongGaugeValue(AzureFileSystemInstrumentation instrumentation,
      String gaugeName) {
	  return getLongGauge(gaugeName, getMetrics(instrumentation));
  }
  
  /**
   * Gets the current value of the given counter.
   */
  public static long getLongCounterValue(AzureFileSystemInstrumentation instrumentation,
      String counterName) {
    return getLongCounter(counterName, getMetrics(instrumentation));
  }


  /**
   * Gets the current value of the wasb_bytes_written_last_second counter.
   */
  public static long getCurrentBytesWritten(AzureFileSystemInstrumentation instrumentation) {
    return getLongGaugeValue(instrumentation, WASB_BYTES_WRITTEN);
  }

  /**
   * Gets the current value of the wasb_bytes_read_last_second counter.
   */
  public static long getCurrentBytesRead(AzureFileSystemInstrumentation instrumentation) {
    return getLongGaugeValue(instrumentation, WASB_BYTES_READ);
  }

  /**
   * Gets the current value of the wasb_raw_bytes_uploaded counter.
   */
  public static long getCurrentTotalBytesWritten(
      AzureFileSystemInstrumentation instrumentation) {
    return getLongCounterValue(instrumentation, WASB_RAW_BYTES_UPLOADED);
  }

  /**
   * Gets the current value of the wasb_raw_bytes_downloaded counter.
   */
  public static long getCurrentTotalBytesRead(
      AzureFileSystemInstrumentation instrumentation) {
    return getLongCounterValue(instrumentation, WASB_RAW_BYTES_DOWNLOADED);
  }

  /**
   * Gets the current value of the asv_web_responses counter.
   */
  public static long getCurrentWebResponses(
      AzureFileSystemInstrumentation instrumentation) {
    return getLongCounter(WASB_WEB_RESPONSES, getMetrics(instrumentation));
  }
}
