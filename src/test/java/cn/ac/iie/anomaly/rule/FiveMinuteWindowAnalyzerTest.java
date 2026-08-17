package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.config.WindowRange;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class FiveMinuteWindowAnalyzerTest {
    private static final DateTimeFormatter F = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Test
    public void offHoursProducesConfiguredTopN() throws Exception {
        Path file = Files.createTempFile("net-traffic-sentinel-analyzer-", ".properties");
        String props =
                "rule.large.enabled=false\n" +
                "rule.offhours.enabled=true\n" +
                "rule.offhours.topn=2\n" +
                "rule.offhours.sketch.capacity=8\n" +
                "rule.offhours.weekday.start.hour=20\n" +
                "rule.offhours.weekday.end.hour=8\n" +
                "alert.logid=STATIC\n" +
                "alert.vendorCode=V001\n" +
                "alert.remark1=\n" +
                "alert.remark2=\n";
        Files.write(file, props.getBytes(StandardCharsets.UTF_8));
        try {
            AppConfig config = AppConfig.load(new String[]{"--config", file.toString()});
            ZoneId zone = ZoneId.of("Asia/Shanghai");
            WindowRange window = WindowRange.fromStart(LocalDateTime.of(2026, 8, 17, 21, 0), 5, zone);
            FiveMinuteWindowAnalyzer analyzer = new FiveMinuteWindowAnalyzer(
                    config, new HistoricalBaselineStore(config), window);
            for (int i = 0; i < 10; i++) {
                LocalDateTime time = LocalDateTime.of(2026, 8, 17, 21, i % 5);
                String text = time.format(F);
                analyzer.add(new MetricRecord(
                        text, time.atZone(zone).toInstant().toEpochMilli(),
                        "10.0.0." + (i % 3), "10.0.1.1", "TCP",
                        i + 1, 1, 1, 10, 10), time);
            }
            List<AlertRecord> alerts = analyzer.finish().getAlerts();
            Assert.assertEquals(2, alerts.size());
            Assert.assertEquals(3, alerts.get(0).getAnomalyType());
            Assert.assertEquals(3, alerts.get(1).getAnomalyType());
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
