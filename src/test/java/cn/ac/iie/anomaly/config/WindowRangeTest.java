package cn.ac.iie.anomaly.config;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;

public class WindowRangeTest {
    @Test
    public void floorsEventTimeToFiveMinuteBoundary() {
        LocalDateTime input = LocalDateTime.of(2026, 8, 17, 11, 37, 42);
        Assert.assertEquals("2026-08-17 11:35:00",
                WindowRange.floorToWindow(input, 5).format(WindowRange.DATETIME));
    }

    @Test
    public void buildsLeftClosedRightOpenBusinessWindow() {
        WindowRange w = WindowRange.fromStart(
                LocalDateTime.of(2026, 8, 17, 20, 35), 5, ZoneId.of("Asia/Shanghai"));
        Assert.assertEquals("2026-08-17 20:35:00", w.startText());
        Assert.assertEquals("2026-08-17 20:40:00", w.endText());
    }

    @Test
    public void legacyDorisFilterCompatibilityIsKeptForOverlayBuilds() {
        WindowRange w = WindowRange.fromStart(
                LocalDateTime.of(2026, 8, 17, 20, 35), 5, ZoneId.of("Asia/Shanghai"));
        String filter = w.toDorisFilter(
                LocalDateTime.of(2026, 8, 17, 20, 36),
                LocalDateTime.of(2026, 8, 17, 20, 37));
        Assert.assertTrue(filter.contains("dayTime = '2026-08-17'"));
        Assert.assertTrue(filter.contains("dayHourTime = '2026-08-17 20:00:00'"));
        Assert.assertTrue(filter.contains("collectTime >= '2026-08-17 20:36:00'"));
        Assert.assertTrue(filter.contains("collectTime < '2026-08-17 20:37:00'"));
    }
}
