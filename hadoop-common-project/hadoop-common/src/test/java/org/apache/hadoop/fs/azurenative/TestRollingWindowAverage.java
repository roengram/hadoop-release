package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;
import org.junit.*;

public class TestRollingWindowAverage {
  /**
   * Tests the basic functionality of the class.
   */
  @Test
  public void testBasicFunctionality() throws Exception {
    RollingWindowAverage average = new RollingWindowAverage(100);
    assertEquals(0, average.getCurrentAverage()); // Nothing there yet.
    average.addPoint(5);
    assertEquals(5, average.getCurrentAverage()); // One point in there.
    Thread.sleep(50);
    average.addPoint(15);
    assertEquals(10, average.getCurrentAverage()); // Two points in there.
    Thread.sleep(60);
    assertEquals(15, average.getCurrentAverage()); // One point retired.
    Thread.sleep(50);
    assertEquals(0, average.getCurrentAverage()); // Both points retired.
  }
}
