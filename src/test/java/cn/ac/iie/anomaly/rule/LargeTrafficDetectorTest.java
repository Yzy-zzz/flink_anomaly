package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.config.WindowRange;
import cn.ac.iie.anomaly.history.ContextKey;
import cn.ac.iie.anomaly.history.ContextStats;
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
    public void type2AlertContainsCompactDetailAndBootstrapLearnsCappedSample() throws Exception {
        Path file = tempConfig(true);
        try {
            AppConfig config = AppConfig.load(new String[]{"--config", file.toString()});
            HistoricalBaselineStore store = new HistoricalBaselineStore(config);
            LocalDateTime currentTime = LocalDateTime.of(2026, 8, 17, 0, 1);
            String contextKey = seedHistoricalContext(store, currentTime);
            ContextStats historicalContext = store.contextStats(
                    contextKey, currentTime.toLocalDate(), 0.999d, 0.999d);

            WindowRange window = WindowRange.fromStart(LocalDateTime.of(2026, 8, 17, 0, 0), 5, ZONE);
            FiveMinuteWindowAnalyzer analyzer = new FiveMinuteWindowAnalyzer(config, store, window);
            MetricRecord record = metric(currentTime, "10.0.0.1", "10.0.0.2", 5000L, 50L);
            analyzer.add(record, currentTime);

            WindowAnalysisResult result = analyzer.finish();
            Assert.assertEquals(1, result.getAlerts().size());
            AlertRecord alert = result.getAlerts().get(0);
            Assert.assertEquals(2, alert.getAnomalyType());

            Map<String, Object> detail = alert.getAnomalyDetail();
            Assert.assertEquals("type=2 detail should stay compact", 8, detail.size());
            Assert.assertEquals(5000L, ((Number) detail.get("current_bytes")).longValue());
            Assert.assertEquals(50L, ((Number) detail.get("current_pkts")).longValue());
            Assert.assertEquals(historicalContext.getP50Bytes(),
                    ((Number) detail.get("baseline_bytes")).longValue());
            Assert.assertEquals(historicalContext.getP50Pkts(),
                    ((Number) detail.get("baseline_pkts")).longValue());
            Assert.assertEquals((double) historicalContext.getThresholdBytes(),
                    ((Number) detail.get("bytes_threshold")).doubleValue(), 0.001d);
            Assert.assertEquals((double) historicalContext.getThresholdPkts(),
                    ((Number) detail.get("pkts_threshold")).doubleValue(), 0.001d);
            Assert.assertEquals(historicalContext.getThresholdBytes() * 2d,
                    ((Number) detail.get("extreme_bytes_threshold")).doubleValue(), 0.001d);
            Assert.assertEquals("BYTES_AND_PKTS", detail.get("anomaly_reason"));

            // Internal algorithm/debug fields must not leak into the external alert contract.
            Assert.assertFalse(detail.containsKey("baseline_source"));
            Assert.assertFalse(detail.containsKey("context_high_quantile_bytes"));
            Assert.assertFalse(detail.containsKey("bytes_baseline_multiplier"));
            Assert.assertFalse(detail.containsKey("pair_learning_mode"));
            Assert.assertFalse(detail.containsKey("bytes_anomaly"));

            store.apply(result.getHistoryUpdate(), window.getEnd(), ZONE);
            long pairHash = ConnectionKey.hash64(record.pairKey());
            HistoricalBaselineStore.PairStats learned = store.pairStats(pairHash, record.getCollectTimestamp());
            Assert.assertNotNull(learned);
            Assert.assertEquals(1L, learned.getSampleCount());
            Assert.assertEquals((double) historicalContext.getThresholdBytes(), learned.getEmaBytes(), 0.001d);
            Assert.assertEquals((double) historicalContext.getThresholdPkts(), learned.getEmaPkts(), 0.001d);
            Assert.assertTrue("bootstrap sample must be capped below the anomaly",
                    historicalContext.getThresholdBytes() < record.totalBytes());
            Assert.assertTrue("bootstrap packet sample must be capped below the anomaly",
                    historicalContext.getThresholdPkts() < record.totalPkts());

            // Keep this assertion so a future ContextKey change cannot silently invalidate the fixture.
            Assert.assertEquals(contextKey, ContextKey.of("UNKNOWN", currentTime, 5));
        } finally {
            Files.deleteIfExists(file);
        }
    }

    @Test
    public void type2ReasonIsExtremeBytesWhenPacketsStayBelowThreshold() throws Exception {
        Path file = tempConfig(true);
        try {
            AppConfig config = AppConfig.load(new String[]{"--config", file.toString()});
            HistoricalBaselineStore store = new HistoricalBaselineStore(config);
            LocalDateTime currentTime = LocalDateTime.of(2026, 8, 17, 0, 1);
            seedHistoricalContext(store, currentTime);

            WindowRange window = WindowRange.fromStart(LocalDateTime.of(2026, 8, 17, 0, 0), 5, ZONE);
            FiveMinuteWindowAnalyzer analyzer = new FiveMinuteWindowAnalyzer(config, store, window);
            // Historical packet threshold is above 5, while bytes is deliberately extreme.
            analyzer.add(metric(currentTime, "10.0.0.7", "10.0.0.8", 5000L, 5L), currentTime);

            WindowAnalysisResult result = analyzer.finish();
            Assert.assertEquals(1, result.getAlerts().size());
            Map<String, Object> detail = result.getAlerts().get(0).getAnomalyDetail();
            Assert.assertEquals(8, detail.size());
            Assert.assertEquals("EXTREME_BYTES", detail.get("anomaly_reason"));
            Assert.assertTrue(((Number) detail.get("current_bytes")).doubleValue()
                    > ((Number) detail.get("extreme_bytes_threshold")).doubleValue());
            Assert.assertTrue(((Number) detail.get("current_pkts")).doubleValue()
                    <= ((Number) detail.get("pkts_threshold")).doubleValue());
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
            Assert.assertEquals(8, detail.size());
            Assert.assertEquals(100L, ((Number) detail.get("baseline_bytes")).longValue());
            Assert.assertEquals(1L, ((Number) detail.get("baseline_pkts")).longValue());
            Assert.assertEquals("BYTES_AND_PKTS", detail.get("anomaly_reason"));
            Assert.assertFalse(detail.containsKey("pair_learning_mode"));
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
                "alert.vendorCode=V001\n" +
                "alert.remark1=\n" +
                "alert.remark2=\n";
        Files.write(file, props.getBytes(StandardCharsets.UTF_8));
        return file;
    }
}
