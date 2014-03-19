package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.annotation.*;
import org.apache.hadoop.metrics2.lib.*;

/**
 * A metrics source for the WASB file system to track all the metrics we care
 * about for getting a clear picture of the performance/reliability/interaction
 * of the Hadoop cluster with Azure Storage.
 */
@Metrics(about="Metrics for WASB", context="azureFileSystem")
final class AzureFileSystemInstrumentation implements MetricsSource {
  static final String WASB_WEB_RESPONSES = "wasb_web_responses";
  static final String WASB_BYTES_WRITTEN =
      "wasb_bytes_written_last_second";
  static final String WASB_BYTES_READ =
      "wasb_bytes_read_last_second";
  static final String WASB_RAW_BYTES_UPLOADED =
      "wasb_raw_bytes_uploaded";
  static final String WASB_RAW_BYTES_DOWNLOADED =
      "wasb_raw_bytes_downloaded";
  static final String WASB_FILES_CREATED = "wasb_files_created";
  static final String WASB_FILES_DELETED = "wasb_files_deleted";
  static final String WASB_DIRECTORIES_CREATED = "wasb_directories_created";
  static final String WASB_DIRECTORIES_DELETED = "wasb_directories_deleted";
  static final String WASB_UPLOAD_RATE =
      "wasb_maximum_upload_bytes_per_second";
  static final String WASB_DOWNLOAD_RATE =
      "wasb_maximum_download_bytes_per_second";
  static final String WASB_UPLOAD_LATENCY =
      "wasb_average_block_upload_latency_ms";
  static final String WASB_DOWNLOAD_LATENCY =
      "wasb_average_block_download_latency_ms";
  static final String WASB_CLIENT_ERRORS = "wasb_client_errors";
  static final String WASB_SERVER_ERRORS = "wasb_server_errors";

  /**
   * Config key for how big the rolling window size for latency metrics should
   * be (in seconds).
   */
  private static final String KEY_ROLLING_WINDOW_SIZE = "fs.azure.metrics.rolling.window.size";

  private final MetricsRegistry registry =
      new MetricsRegistry("azureFileSystem")
      .setContext("azureFileSystem");

  @Metric({ WASB_WEB_RESPONSES, "Total number of web responses obtained from Azure Storage" })
  MutableCounterLong numberOfWebResponses;
  @Metric({ WASB_FILES_CREATED, "Total number of files created through the WASB file system." })
  MutableCounterLong numberOfFilesCreated;
  @Metric({ WASB_FILES_DELETED, "Total number of files deleted through the WASB file system." })
  MutableCounterLong numberOfFilesDeleted;
  @Metric({ WASB_DIRECTORIES_CREATED, "Total number of directories created through the WASB file system." })
  MutableCounterLong numberOfDirectoriesCreated;
  @Metric({ WASB_DIRECTORIES_DELETED, "Total number of directories deleted through the WASB file system." })
  MutableCounterLong numberOfDirectoriesDeleted;
  @Metric({ WASB_BYTES_WRITTEN, "Total number of bytes written to Azure Storage during the last second." })
  MutableGaugeLong bytesWrittenInLastSecond;
  @Metric({ WASB_BYTES_READ, "Total number of bytes read from Azure Storage during the last second." })
  MutableGaugeLong bytesReadInLastSecond;
  @Metric({ WASB_UPLOAD_RATE, "The maximum upload rate encountered to Azure Storage in bytes/second." })
  MutableGaugeLong maximumUploadBytesPerSecond;
  @Metric({ WASB_DOWNLOAD_RATE, "The maximum download rate encountered to Azure Storage in bytes/second." })
  MutableGaugeLong maximumDownloadBytesPerSecond;
  @Metric({ WASB_RAW_BYTES_UPLOADED, "Total number of raw bytes (including overhead) uploaded to Azure Storage." })
  MutableCounterLong rawBytesUploaded;
  @Metric({ WASB_RAW_BYTES_DOWNLOADED, "Total number of raw bytes (including overhead) downloaded from Azure Storage." })
  MutableCounterLong rawBytesDownloaded;
  @Metric({ WASB_CLIENT_ERRORS, "Total number of client-side errors by WASB (excluding 404)." })
  MutableCounterLong clientErrors;
  @Metric({ WASB_SERVER_ERRORS, "Total number of server-caused errors by WASB." })
  MutableCounterLong serverErrors;
  @Metric({ WASB_UPLOAD_LATENCY, "The average latency in milliseconds of uploading a single block." +
  		" The average latency is calculated over a rolling window." })
  MutableGaugeLong averageBlockUploadLatencyMs;
  @Metric({ WASB_DOWNLOAD_LATENCY, "The average latency in milliseconds of downloading a single block." +
      " The average latency is calculated over a rolling window." })
  MutableGaugeLong averageBlockDownloadLatencyMs;

  private long currentMaximumUploadBytesPerSecond;
  private long currentMaximumDownloadBytesPerSecond;
  private static final int DEFAULT_LATENCY_ROLLING_AVERAGE_WINDOW =
      5; // seconds
  private final RollingWindowAverage currentBlockUploadLatency;
  private final RollingWindowAverage currentBlockDownloadLatency;
  private UUID fileSystemInstanceId;

  public AzureFileSystemInstrumentation(Configuration conf) {
    fileSystemInstanceId = UUID.randomUUID();
    registry.tag("wasbFileSystemId",
        "A unique identifier for the file ",
        fileSystemInstanceId.toString());
    final int rollingWindowSizeInSeconds =
        conf.getInt(KEY_ROLLING_WINDOW_SIZE,
            DEFAULT_LATENCY_ROLLING_AVERAGE_WINDOW);
    currentBlockUploadLatency =
        new RollingWindowAverage(rollingWindowSizeInSeconds * 1000);
    currentBlockDownloadLatency =
        new RollingWindowAverage(rollingWindowSizeInSeconds * 1000);
  }

  /**
   * The unique identifier for this file system in the metrics.
   */
  public UUID getFileSystemInstanceId() {
    return fileSystemInstanceId;
  }

  /**
   * Sets the account name to tag all the metrics with.
   * @param accountName The account name.
   */
  public void setAccountName(String accountName) {
    registry.tag("accountName",
        "Name of the Azure Storage account that these metrics are going against",
        accountName);
  }

  /**
   * Sets the container name to tag all the metrics with.
   * @param containerName The container name.
   */
  public void setContainerName(String containerName) {
    registry.tag("containerName",
        "Name of the Azure Storage container that these metrics are going against",
        containerName);
  }

  /**
   * Indicate that we just got a web response from Azure Storage. This should
   * be called for every web request/response we do (to get accurate metrics
   * of how we're hitting the storage service).
   */
  public void webResponse() {
    numberOfWebResponses.incr();
  }

  /**
   * Indicate that we just created a file through WASB.
   */
  public void fileCreated() {
    numberOfFilesCreated.incr();
  }

  /**
   * Indicate that we just deleted a file through WASB.
   */
  public void fileDeleted() {
    numberOfFilesDeleted.incr();
  }

  /**
   * Indicate that we just created a directory through WASB.
   */
  public void directoryCreated() {
    numberOfDirectoriesCreated.incr();
  }

  /**
   * Indicate that we just deleted a directory through WASB.
   */
  public void directoryDeleted() {
    numberOfDirectoriesDeleted.incr();
  }

  /**
   * Sets the current gauge value for how many bytes were written in the last
   *  second.
   * @param currentBytesWritten The number of bytes.
   */
  public void updateBytesWrittenInLastSecond(long currentBytesWritten) {
    bytesWrittenInLastSecond.set(currentBytesWritten);
  }

  /**
   * Sets the current gauge value for how many bytes were read in the last
   *  second.
   * @param currentBytesRead The number of bytes.
   */
  public void updateBytesReadInLastSecond(long currentBytesRead) {
    bytesReadInLastSecond.set(currentBytesRead);
  }

  /**
   * Record the current bytes-per-second upload rate seen.
   * @param bytesPerSecond The bytes per second.
   */
  public synchronized void currentUploadBytesPerSecond(long bytesPerSecond) {
    if (bytesPerSecond > currentMaximumUploadBytesPerSecond) {
      currentMaximumUploadBytesPerSecond = bytesPerSecond;
      maximumUploadBytesPerSecond.set(bytesPerSecond);
    }
  }

  /**
   * Record the current bytes-per-second download rate seen.
   * @param bytesPerSecond The bytes per second.
   */
  public synchronized void currentDownloadBytesPerSecond(long bytesPerSecond) {
    if (bytesPerSecond > currentMaximumDownloadBytesPerSecond) {
      currentMaximumDownloadBytesPerSecond = bytesPerSecond;
      maximumDownloadBytesPerSecond.set(bytesPerSecond);
    }
  }

  /**
   * Indicate that we just uploaded some data to Azure storage.
   * @param numberOfBytes The raw number of bytes uploaded (including overhead).
   */
  public void rawBytesUploaded(long numberOfBytes) {
    rawBytesUploaded.incr(numberOfBytes);
  }

  /**
   * Indicate that we just downloaded some data to Azure storage.
   * @param numberOfBytes The raw number of bytes downloaded (including overhead).
   */
  public void rawBytesDownloaded(long numberOfBytes) {
    rawBytesDownloaded.incr(numberOfBytes);
  }

  /**
   * Indicate that we just uploaded a block and record its latency.
   * @param latency The latency in milliseconds.
   */
  public void blockUploaded(long latency) {
    currentBlockUploadLatency.addPoint(latency);
  }

  /**
   * Indicate that we just downloaded a block and record its latency.
   * @param latency The latency in milliseconds.
   */
  public void blockDownloaded(long latency) {
    currentBlockDownloadLatency.addPoint(latency);
  }

  /**
   * Indicate that we just encountered a client-side error.
   */
  public void clientErrorEncountered() {
    clientErrors.incr();
  }

  /**
   * Indicate that we just encountered a server-caused error.
   */
  public void serverErrorEncountered() {
    serverErrors.incr();
  }

  /**
   * Get the current rolling average of the upload latency.
   * @return rolling average of upload latency in milliseconds.
   */
  public long getBlockUploadLatency () {
    return currentBlockUploadLatency.getCurrentAverage();
  }

  /**
   * Get the current rolling average of the download latency.
   * @return rolling average of download latency in milliseconds.
   */
  public long getBlockDownloadLatency() {
    return currentBlockDownloadLatency.getCurrentAverage();
  }

  /**
   * Get the current maximum upload bandwidth.
   * @return maximum upload bandwidth in bytes per second.
   */
  public long getCurrentMaximumUploadBandwidth() {
    return currentMaximumUploadBytesPerSecond;
  }

  /**
   * Get the current maximum download bandwidth.
   * @return maximum download bandwidth in bytes per second.
   */
  public long getCurrentMaximumDownloadBandwidth() {
    return currentMaximumDownloadBytesPerSecond;

  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    averageBlockDownloadLatencyMs.set(
        currentBlockDownloadLatency.getCurrentAverage());
    averageBlockUploadLatencyMs.set(
        currentBlockUploadLatency.getCurrentAverage());
    registry.snapshot(collector.addRecord(registry.info()), all);
  }
}
