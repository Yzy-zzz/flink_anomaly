package cn.ac.iie.anomaly.util;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;

public class TimeUtilsTest {
    @Test
    public void weekdayNightIsOffHours() {
        Assert.assertTrue(TimeUtils.isOffHours(LocalDateTime.of(2026, 8, 17, 21, 0), 20, 8));
        Assert.assertTrue(TimeUtils.isOffHours(LocalDateTime.of(2026, 8, 17, 7, 59), 20, 8));
        Assert.assertFalse(TimeUtils.isOffHours(LocalDateTime.of(2026, 8, 17, 8, 0), 20, 8));
        Assert.assertFalse(TimeUtils.isOffHours(LocalDateTime.of(2026, 8, 17, 19, 59), 20, 8));
    }

    @Test
    public void weekendIsAlwaysOffHours() {
        Assert.assertTrue(TimeUtils.isOffHours(LocalDateTime.of(2026, 8, 16, 12, 0), 20, 8));
    }
}
