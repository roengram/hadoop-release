package org.apache.hadoop.fs.azurenative;

import java.util.*;

/**
 * Helper class to calculate rolling-window averages.
 * Used to calculate rolling-window metrics in AzureNativeFileSystem.
 */
final class RollingWindowAverage {
  private final ArrayDeque<DataPoint> currentPoints =
      new ArrayDeque<DataPoint>();
  private final long windowSizeMs;

  /**
   * Create a new rolling-window average for the given window size.
   * @param windowSizeMs The size of the window in milliseconds.
   */
  public RollingWindowAverage(long windowSizeMs) {
    this.windowSizeMs = windowSizeMs;
  }

  /**
   * Add a new data point that just happened.
   * @param value The value of the data point.
   */
  public synchronized void addPoint(long value) {
    currentPoints.offer(new DataPoint(new Date(), value));
    cleanupOldPoints();
  }

  /**
   * Get the current average.
   * @return The current average.
   */
  public synchronized long getCurrentAverage() {
    cleanupOldPoints();
    if (currentPoints.isEmpty()) {
      return 0;
    }
    long sum = 0;
    for (DataPoint current : currentPoints) {
      sum += current.value;
    }
    return sum / currentPoints.size();
  }

  /**
   * Clean up points that don't count any more (are before our
   * rolling window) from our current queue of points.
   */
  private void cleanupOldPoints() {
    Date cutoffTime = new Date(new Date().getTime() - windowSizeMs);
    while (!currentPoints.isEmpty() &&
        currentPoints.peekFirst().eventTime.before(cutoffTime)) {
      currentPoints.removeFirst();
    }
  }

  /**
   * A single data point.
   */
  private static class DataPoint {
    Date eventTime;
    long value;

    public DataPoint(Date eventTime, long value) {
      this.eventTime = eventTime;
      this.value = value;
    }
  }
}
