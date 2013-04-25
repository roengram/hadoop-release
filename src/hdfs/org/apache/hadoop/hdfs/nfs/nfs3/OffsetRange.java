/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.nfs.nfs3;

/**
 * OffsetRange is the range of read/write request. A single point (e.g.,[5,5])
 * is not a valid range.
 */
public class OffsetRange implements Comparable<OffsetRange> {
  private final long min;
  private final long max;

  OffsetRange(long min, long max) {
    if ((min >= max) || (min < 0) || (max < 0)) {
      throw new IllegalArgumentException("Wrong offset range: (" + min + ","
          + max + ")");
    }
    this.min = min;
    this.max = max;
  }

  long getMin() {
    return min;
  }

  long getMax() {
    return max;
  }

  @Override
  public int hashCode() {
    return (int) (min ^ max);
  }

  @Override
  public boolean equals(Object o) {
    assert (o instanceof OffsetRange);
    OffsetRange range = (OffsetRange) o;
    return (min == range.getMin()) && (max == range.getMax());
  }

  /**
   * There are only 4 kinds of overlap(but not identical) cases for two ranges,
   * r1 and r2 : (a) r1 is in r2; (b) r2 is in r1; (c) r1 overlaps left part of
   * r2; (d) r1 overlaps right part of r2.
   */
  boolean hasOverlap(OffsetRange range) {
    long rangeMin = range.getMin();
    long rangeMax = range.getMax();
    // Rule out the identical range
    if ((min == rangeMin) && (max == rangeMax)) {
      return false;
    }
    
    // For non-identical range, same min or max means overlap
    if ((min == rangeMin) || (max == rangeMax)) {
      return true;
    }
    
    if ((min < rangeMin) && (max > rangeMin)) {
      return true; // 2 cases: max > rangeMin, max > rangeMax
    }    
    if ((min > rangeMin) && (min < rangeMax)) {
      return true; // 2 cases: max < rangeMax, max > rangeMax
    }
    return false;
  }

  // 0: identical -1: on the left 1: on the right 2: overlapped
  public int compareTo(OffsetRange other) {
    if (hasOverlap(other)) {
      return 2;
    }

    if (min < other.getMin()) {
      return -1;
    } else if (min > other.getMin()) {
      return 1;
    } else {
      assert (max == other.getMax());
      return 0;
    }
  }
}
