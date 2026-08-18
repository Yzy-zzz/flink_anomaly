package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.config.WindowRange;
import cn.ac.iie.anomaly.history.ContextKey;
import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.util.ConnectionKey;
import com.tdunning.math.stats.TDigest;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class LargeTrafficDetectorTest {
    private static final DateTimeFormatter F = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ZoneId ZONE = ZoneId.of("Asia/Shanghai");

    @Test
    public void type2AlertContainsFullEvidenceAndBootstrapLearnsCappedSample() throws Exception {
        Path file = tempConfig(true);
        try {
            AppConfig config = AppConfig.load(new String[]{"--config", file.toString()});
            HistoricalBaselineStore store = new HistoricalBaselineStore(config);
            LocalDateTime currentTime = LocalDateTime.of(2026, 8, 17, 0, 1);
            String contextKey = seedHistoricalContext(store, currentTime);

            WindowRange window = WindowRange.fromStart(LocalDateTime.of(2026, 8, 17, 0, 0), 5, ZONE);
            FiveMinuteWindowAnalyzer analyzer = new FiveMinuteWindowAnalyzer(config, store, window);
            MetricRecord record = metric(currentTime, "10.0.0.1", "10.0.0.2", 5000L, 50L);
            analyzer.add(record, currentTime);

            WindowAnalysisResult result = analyzer.finish();
            Assert.assertEquals(1, result.getAlerts().size());
            AlertRecord alert = result.getAlerts().get(0);
            Assert.assertEquals(2, alert.getAnomalyType());

            Map<String, Object> detail = alert.getAnomalyDetail();
            Assert.assertEquals("HISTORICAL_CONTEXT_P50", detail.get("baseline_source"));
            Assert.assertEquals("HISTORICAL", detail.get("context_source"));
            Assert.assertEquals(0L, ((Number) detail.get("pair_sample_count")).longValue());
            Assert.assertEquals(3, ((Number) detail.get("pair_min_samples")).intValue());
            Assert.assertEquals("CAPPED_BOOTSTRAP", detail.get("pair_learning_mode"));
            Assert.assertTrue((Boolean) detail.get("bytes_anomaly"));
            Assert.assertTrue((Boolean) detail.get("pkts_anomaly"));
            Assert.assertTrue(detail.containsKey("bytes_threshold"));
            Assert.assertTrue(detail.containsKey("pkts_threshold"));
            Assert.assertTrue(detail.containsKey("context_p50_bytes"));
            Assert.assertTrue(detail.containsKey("context_p90_bytes"));
            Assert.assertTrue(detail.containsKey("context_high_quantile_bytes"));
            Assert.assertEquals(0.999d, ((Number) detail.get("context_bytes_quantile")).doubleValue(), 0.0d);

            long learningCapBytes = ((Number) detail.get("pair_learning_cap_bytes")).longValue();
            long learningCapPkts = ((Number) detail.get("pair_learning_cap_pkts")).longValue();
            Assert.assertEquals(((Number) detail.get("context_high_quantile_bytes")).longValue(), learningCapBytes);
            Assert.assertEquals(((Number) detail.get("context_high_quantile_pkts")).longValue(), learningCapPkts);
            Assert.assertTrue("bootstrap sample must be capped below the anomaly", learningCapBytes < record.totalBytes());
            Assert.assertTrue("bootstrap packet sample must be capped below the anomaly", learningCapPkts < record.totalPkts());

            store.apply(result.getHistoryUpdate(), window.getEnd(), ZONE);
            long pairHash = ConnectionKey.hash64(record.pairKey());
            HistoricalBaselineStore.PairStats learned = store.pairStats(pairHash, record.getCollectTimestamp());
            Assert.assertNotNull(learned);
            Assert.assertEquals(1L, learned.getSampleCount());
            Assert.assertEquals((double) learningCapBytes, learned.getEmaBytes(), 0.001d);
            Assert.assertEquals((double) learningCapPkts, learned.getEmaPkts(), 0.001d);

            // Keep this assertion so a future ContextKey change cannot silently invalidate the fixture.
            Assert.assertEquals(contextKey, ContextKey.of("UNKNOWN", currentTime, 5));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void matureAnomalousPairStillDoesNotLearn() throws Exception {
        Path file = tempConfig(true);
        try {
            AppConfig config = AppConfig.load(new String[]{"--config", file.toString()});
            HistoricalBaselineStore store = new HistoricalBaselineStore(config);
            LocalDateTime currentTime = LocalDateTime.of(2026, 8, 17, 0, 1);
            seedHistoricalContext(store, currentTime);

            MetricRecord record = metric(currentTime, "10.0.0.3", "10.0.0.4", 5000L, 50L);
            long pairHash = ConnectionKey.hash64(record.pairKey());
            HistoricalBaselineStore.WindowUpdate pairSeed = new HistoricalBaselineStore.WindowUpdate();
            for (int i = 0; i < 3; i++) {
                LocalDateTime t = LocalDateTime.of(2026, 8, 14, 0, i + 1);
                pairSeed.addPairSample(new HistoricalBaselineStore.PairSample(
                        pairHash, 100L, 1L, t.atZone(ZONE).toInstant().toEpochMilli()));
            }
            store.apply(pairSeed, LocalDateTime.of(2026, 8, 14, 0, 5), ZONE);
            Assert.assertEquals(3L, store.pairStats(pairHash, record.getCollectTimestamp()).getSampleCount());

            WindowRange window = WindowRange.fromStart(LocalDateTime.of(2026, 8, 17, 0, 0), 5, ZONE);
            FiveMinuteWindowAnalyzer analyzer = new FiveMinuteWindowAnalyzer(config, store, window);
            analyzer.add(record, currentTime);
            WindowAnalysisResult result = analyzer.finish();

            Assert.assertEquals(1, result.getAlerts().size());
            Map<String, Object> detail = result.getAlerts().get(0).getAnomalyDetail();
            Assert.assertEquals("PAIR_EMA", detail.get("baseline_source"));
            Assert.assertEquals(3L, ((Number) detail.get("pair_sample_count")).longValue());
            Assert.assertEquals("SKIP_ANOMALOUS_MATURE", detail.get("pair_learning_mode"));
            Assert.assertFalse(detail.containsKey("pair_learning_cap_bytes"));
            Assert.assertFalse(detail.containsKey("pair_learning_cap_pkts"));

            store.apply(result.getHistoryUpdate(), window.getEnd(), ZONE);
            HistoricalBaselineStore.PairStats after = store.pairStats(pairHash, record.getCollectTimestamp());
            Assert.assertNotNull(after);
            Assert.assertEquals("mature anomaly must not increment pair sample count", 3L, after.getSampleCount());
            Assert.assertEquals(100d, after.getEmaBytes(), 0.001d);
            Assert.assertEquals(1d, after.getEmaPkts(), 0.001d);
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static String seedHistoricalContext(HistoricalBaselineStore store, LocalDateTime currentTime) {
        String contextKey = ContextKey.of("UNKNOWN", currentTime, 5);
        TDigest bytes = TDigest.createMergingDigest(100d);
        TDigest pkts = TDigest.createMergingDigest(100d);
        for (int i = 0; i < 100; i++) {
            bytes.add(100L);
            pkts.add(1L);
        }
        bytes.add(1000L);
        pkts.add(10L);

        HistoricalBaselineStore.WindowUpdate contextSeed = new HistoricalBaselineStore.WindowUpdate();
        contextSeed.addContext(new HistoricalBaselineStore.ContextUpdate(
                contextKey, LocalDate.of(2026, 8, 14).toEpochDay(), bytes, pkts, 101L));
        store.apply(contextSeed, LocalDateTime.of(2026, 8, 14, 0, 5), ZONE);
        return contextKey;
    }

    private static MetricRecord metric(LocalDateTime time, String src, String dst, long bytes, long pkts) {
        return new MetricRecord(
                time.format(F),
                time.atZone(ZONE).toInstant().toEpochMilli(),
                src,
                dst,
                "UNKNOWN",
                1L,
                pkts / 2L,
                pkts - pkts / 2L,
                bytes / 2L,
                bytes - bytes / 2L);
    }

    private static Path tempConfig(boolean cappedBootstrapLearning) throws Exception {
        Path file = Files.createTempFile("large-traffic-detector-", ".properties");
        String props =
                "rule.large.enabled=true\n" +
                "rule.offhours.enabled=false\n" +
                "rule.large.bytes.quantile=0.999\n" +
                "rule.large.pkts.quantile=0.999\n" +
                "rule.large.bytes.baseline.multiplier=4.0\n" +
                "rule.large.pkts.baseline.multiplier=4.0\n" +
                "rule.large.extreme.multiplier=2.0\n" +
                "rule.large.candidate.capacity=100\n" +
                "rule.large.tdigest.compression=100\n" +
                "history.context.enabled=true\n" +
                "history.context.retention.days=7\n" +
                "history.context.min.days=1\n" +
                "history.context.tdigest.compression=100\n" +
                "history.context.slot.minutes=5\n" +
                "history.pair.enabled=true\n" +
                "history.pair.ttl.days=7\n" +
                "history.pair.max.entries=1000\n" +
                "history.pair.ema.alpha=0.1\n" +
                "history.pair.min.samples=3\n" +
                "history.pair.bootstrap.anomaly.capped.learning.enabled=" + cappedBootstrapLearning + "\n" +
                "alert.logid=STATIC\n" +
                "alert.vendorCode=V001\n" +
                "alert.remark1=\n" +
                "alert.remark2=\n";
        Files.write(file, props.getBytes(StandardCharsets.UTF_8));
        return file;
    }
}
