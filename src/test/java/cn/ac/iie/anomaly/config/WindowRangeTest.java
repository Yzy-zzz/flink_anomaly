package cn.ac.iie.anomaly.config;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

public class WindowRangeTest {
    @Test
    public void latestClosedWindowHonorsLandingDelay() throws Exception {
        Path file = Files.createTempFile("net-traffic-sentinel-", ".properties");
        Files.write(file, (
                "business.timezone=Asia/Shanghai\n" +
                "window.size.minutes=5\n" +
                "window.delay.minutes=1\n" +
                "source.start.mode=latest_closed\n").getBytes(StandardCharsets.UTF_8));
        try {
            AppConfig config = AppConfig.load(new String[]{"--config", file.toString()});
            Clock clock = Clock.fixed(Instant.parse("2026-08-17T12:11:30Z"), ZoneOffset.UTC);
            Assert.assertEquals("2026-08-17 20:10:00",
                    WindowRange.latestClosedEnd(config, clock).format(WindowRange.DATETIME));
            Assert.assertEquals("2026-08-17 20:05:00",
                    WindowRange.initialStart(config, clock).format(WindowRange.DATETIME));
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
