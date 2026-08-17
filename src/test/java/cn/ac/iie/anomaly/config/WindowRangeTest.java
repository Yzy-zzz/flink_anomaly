package cn.ac.iie.anomaly.config;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class WindowRangeTest {
    @Test
    public void floorsDorisWatermarkToFiveMinuteBoundary() {
        LocalDateTime maxAfterStableDelay = LocalDateTime.of(2026, 8, 17, 11, 37, 42);
        Assert.assertEquals("2026-08-17 11:35:00",
                WindowRange.floorToWindow(maxAfterStableDelay, 5).format(WindowRange.DATETIME));
    }

    @Test
    public void dorisFilterUsesExactFiveMinuteCollectTimeRange() {
        WindowRange w = WindowRange.fromStart(
                LocalDateTime.of(2026, 8, 17, 20, 35), 5, ZoneId.of("Asia/Shanghai"));
        String filter = w.toDorisFilter();
        Assert.assertTrue(filter.contains("collectTime >= '2026-08-17 20:35:00'"));
        Assert.assertTrue(filter.contains("collectTime < '2026-08-17 20:40:00'"));
        Assert.assertTrue(filter.contains("dayHourTime >= '2026-08-17 20:00:00'"));
        Assert.assertTrue(filter.contains("dayHourTime < '2026-08-17 21:00:00'"));
    }
}
