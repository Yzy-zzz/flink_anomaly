package cn.ac.iie.anomaly.history;

import cn.ac.iie.anomaly.config.AppConfig;
import com.tdunning.math.stats.TDigest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class HistoricalBaselineStoreTest {
    @Test
    public void pairEmaUpdatesAndExpiresByEventTime() throws Exception {
        Path file = tempConfig();
        try {
            AppConfig config = AppConfig.load(new String[]{"--config", file.toString()});
            HistoricalBaselineStore store = new HistoricalBaselineStore(config);
            ZoneId zone = ZoneId.of("Asia/Shanghai");
            long t1 = LocalDateTime.of(2026, 8, 10, 10, 0).atZone(zone).toInstant().toEpochMilli();

            HistoricalBaselineStore.WindowUpdate u1 = new HistoricalBaselineStore.WindowUpdate();
            u1.addPairSample(new HistoricalBaselineStore.PairSample(123L, 1000L, 10L, t1));
            store.apply(u1, LocalDateTime.of(2026, 8, 10, 10, 5), zone);

            HistoricalBaselineStore.PairStats stats = store.pairStats(123L, t1);
            Assert.assertNotNull(stats);
            Assert.assertEquals(1000d, stats.getEmaBytes(), 0.001d);

            long expiredRef = LocalDateTime.of(2026, 8, 18, 10, 0).atZone(zone).toInstant().toEpochMilli();
            Assert.assertNull(store.pairStats(123L, expiredRef));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void contextUsesPreviousDateBuckets() throws Exception {
        Path file = tempConfig();
        try {
            AppConfig config = AppConfig.load(new String[]{"--config", file.toString()});
            HistoricalBaselineStore store = new HistoricalBaselineStore(config);
            ZoneId zone = ZoneId.of("Asia/Shanghai");
            String key = "TLS\u001fWORKDAY\u001f100";

            for (int d = 10; d <= 11; d++) {
                TDigest b = TDigest.createMergingDigest(100d);
                TDigest p = TDigest.createMergingDigest(100d);
                b.add(d == 10 ? 1000 : 1200);
                p.add(d == 10 ? 10 : 12);
                HistoricalBaselineStore.WindowUpdate update = new HistoricalBaselineStore.WindowUpdate();
                update.addContext(new HistoricalBaselineStore.ContextUpdate(
                        key, LocalDate.of(2026, 8, d).toEpochDay(), b, p, 1L));
                store.apply(update, LocalDateTime.of(2026, 8, d, 10, 5), zone);
            }

            ContextStats stats = store.contextStats(key, LocalDate.of(2026, 8, 12), 0.999d, 0.999d);
            Assert.assertTrue(stats.isUsable());
            Assert.assertEquals(2, stats.getContributingDays());
            Assert.assertTrue(stats.getP50Bytes() >= 1000L);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static Path tempConfig() throws Exception {
        Path file = Files.createTempFile("history-", ".properties");
        String props =
                "history.context.enabled=true\n" +
                "history.context.retention.days=7\n" +
                "history.context.min.days=2\n" +
                "history.context.tdigest.compression=100\n" +
                "history.pair.enabled=true\n" +
                "history.pair.ttl.days=7\n" +
                "history.pair.max.entries=1000\n" +
                "history.pair.ema.alpha=0.1\n" +
                "history.pair.min.samples=3\n";
        Files.write(file, props.getBytes(StandardCharsets.UTF_8));
        return file;
    }
}
