package cn.ac.iie.anomaly.source;

import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class DorisPollingAlertSourceTest {

    @Test
    public void watermarkLowerDateUsesRecentLookbackWhenCursorIsWithinRange() {
        LocalDate lower = DorisPollingAlertSource.resolveWatermarkLowerDate(
                LocalDate.of(2026, 8, 20),
                3,
                LocalDateTime.of(2026, 8, 19, 10, 0),
                "latest_data",
                "");

        assertEquals(LocalDate.of(2026, 8, 18), lower);
    }

    @Test
    public void watermarkLowerDateExtendsBackToRestoredCursor() {
        LocalDate lower = DorisPollingAlertSource.resolveWatermarkLowerDate(
                LocalDate.of(2026, 8, 20),
                3,
                LocalDateTime.of(2026, 8, 17, 15, 55),
                "latest_data",
                "");

        assertEquals(LocalDate.of(2026, 8, 17), lower);
    }

    @Test
    public void watermarkLowerDateExtendsBackToFixedStartBeforeCursorInitialization() {
        LocalDate lower = DorisPollingAlertSource.resolveWatermarkLowerDate(
                LocalDate.of(2026, 8, 20),
                3,
                null,
                "fixed",
                "2026-08-16 09:00:00");

        assertEquals(LocalDate.of(2026, 8, 16), lower);
    }

    @Test(expected = IllegalArgumentException.class)
    public void fixedModeRequiresStartTime() {
        DorisPollingAlertSource.resolveWatermarkLowerDate(
                LocalDate.of(2026, 8, 20),
                3,
                null,
                "fixed",
                "");
    }
    @Test
    public void liveDataStillUsesConfiguredStableDelay() {
        LocalDateTime safeEnd = DorisPollingAlertSource.calculateSafeWindowEnd(
                LocalDateTime.of(2026, 8, 20, 12, 37),
                LocalDateTime.of(2026, 8, 20, 12, 40),
                60,
                5,
                true,
                1440);

        assertEquals(LocalDateTime.of(2026, 8, 20, 11, 35), safeEnd);
    }

    @Test
    public void historicalStaleDataReleasesCompleteTailUpToMaxBoundary() {
        LocalDateTime safeEnd = DorisPollingAlertSource.calculateSafeWindowEnd(
                LocalDateTime.of(2026, 8, 17, 16, 57),
                LocalDateTime.of(2026, 8, 20, 0, 5),
                60,
                5,
                true,
                1440);

        assertEquals(LocalDateTime.of(2026, 8, 17, 16, 55), safeEnd);
    }

    @Test
    public void historicalTailReleaseCanBeDisabled() {
        LocalDateTime safeEnd = DorisPollingAlertSource.calculateSafeWindowEnd(
                LocalDateTime.of(2026, 8, 17, 16, 57),
                LocalDateTime.of(2026, 8, 20, 0, 5),
                60,
                5,
                false,
                1440);

        assertEquals(LocalDateTime.of(2026, 8, 17, 15, 55), safeEnd);
    }

}
