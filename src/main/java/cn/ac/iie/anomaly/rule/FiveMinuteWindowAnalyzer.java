package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.config.WindowRange;
import cn.ac.iie.anomaly.history.ContextKey;
import cn.ac.iie.anomaly.history.ContextStats;
import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.util.TimeUtils;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 【导读：一个 5 分钟窗口的“总分析器”】
 *
 * DorisPollingAlertSource 每开始处理一个新窗口，就 new 一个本类。
 * add() 负责逐行累计；finish() 负责在窗口全部读完后统一做最终判定。
 * 这样可以保证“算法读取历史”和“历史真正更新”是两个阶段，避免当前窗口边读边污染基线。
 *
 * In-memory analyzer for one five-minute Doris query.
 * Long-lived history is read-only while the query runs; a bounded update plan is returned at finish().
 */
public final class FiveMinuteWindowAnalyzer implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean largeEnabled;
    private final boolean offHoursEnabled;
    private final int weekdayStartHour;
    private final int weekdayEndHour;
    private final int contextSlotMinutes;
    private final double bytesQuantile;
    private final double pktsQuantile;
    private final LocalDate currentDate;
    private final HistoricalBaselineStore historyStore;

    private final LargeTrafficAccumulator largeAccumulator;
    private final OffHoursAccumulator offHoursAccumulator;
    private final LargeTrafficDetector largeDetector;
    private final OffHoursDetector offHoursDetector;
    private final Map<String, ContextStats> contextStatsCache = new HashMap<>();
    private long rowCount;
    private long offHoursRowCount;

    public FiveMinuteWindowAnalyzer(AppConfig config, HistoricalBaselineStore historyStore, WindowRange window) {
        this.largeEnabled = config.getBoolean("rule.large.enabled", true);
        this.offHoursEnabled = config.getBoolean("rule.offhours.enabled", true);
        this.weekdayStartHour = config.getInt("rule.offhours.weekday.start.hour", 20);
        this.weekdayEndHour = config.getInt("rule.offhours.weekday.end.hour", 8);
        this.contextSlotMinutes = config.getInt("history.context.slot.minutes", 5);
        this.bytesQuantile = config.getDouble("rule.large.bytes.quantile", 0.999d);
        this.pktsQuantile = config.getDouble("rule.large.pkts.quantile", 0.999d);
        this.currentDate = window.getStart().toLocalDate();
        this.historyStore = historyStore;

        if (largeEnabled) {
            this.largeAccumulator = new LargeTrafficAccumulator(
                    config.getDouble("rule.large.tdigest.compression", 200d),
                    config.getInt("rule.large.candidate.capacity", 20000));
            this.largeDetector = new LargeTrafficDetector(config);
        } else {
            this.largeAccumulator = null;
            this.largeDetector = null;
        }

        if (offHoursEnabled) {
            this.offHoursAccumulator = new OffHoursAccumulator(
                    config.getInt("rule.offhours.sketch.capacity", 2048));
            this.offHoursDetector = new OffHoursDetector(config);
        } else {
            this.offHoursAccumulator = null;
            this.offHoursDetector = null;
        }
    }

    /** 每来一条 Doris 指标记录就调用一次；这里只做累计，不输出最终告警。 */
    public void add(MetricRecord record, LocalDateTime collectLocalTime) {
        rowCount++;
        if (largeEnabled) {
            // type=2 的 Context = 协议 + 工作日/周末 + 时间槽。
            // 注意：Context 不包含 srcIp/dstIp，因此同一 Context 下很多不同 IP 对共享一个分布基线。
            String contextKey = ContextKey.of(record.getProtocol(), collectLocalTime, contextSlotMinutes);
            ContextStats stats = contextStatsCache.get(contextKey);
            if (stats == null) {
                stats = historyStore.contextStats(contextKey, currentDate, bytesQuantile, pktsQuantile);
                contextStatsCache.put(contextKey, stats);
            }
            largeAccumulator.add(record, contextKey, stats);
        }
        if (offHoursEnabled && TimeUtils.isOffHours(collectLocalTime, weekdayStartHour, weekdayEndHour)) {
            offHoursAccumulator.add(record);
            offHoursRowCount++;
        }
    }

    /**
     * 窗口读取完毕后统一收尾：
     * 1) type=2 做最终阈值判断并生成 Pair 学习样本；
     * 2) 生成 Context 历史更新；
     * 3) type=3 输出非工作时间 Top-N；
     * 4) 返回给 Source，等 Source 在 checkpointLock 内一次性 apply。
     */
    public WindowAnalysisResult finish() {
        List<AlertRecord> alerts = new ArrayList<>();
        HistoricalBaselineStore.WindowUpdate historyUpdate = new HistoricalBaselineStore.WindowUpdate();

        if (largeEnabled) {
            LargeTrafficDetector.DetectionResult largeResult = largeDetector.detect(largeAccumulator, historyStore);
            alerts.addAll(largeResult.getAlerts());
            for (HistoricalBaselineStore.PairSample pairSample : largeResult.getPairSamples()) {
                historyUpdate.addPairSample(pairSample);
            }
            for (HistoricalBaselineStore.ContextUpdate contextUpdate : largeAccumulator.contextUpdates(currentDate)) {
                historyUpdate.addContext(contextUpdate);
            }
        }
        if (offHoursEnabled && offHoursRowCount > 0L) {
            alerts.addAll(offHoursDetector.detect(offHoursAccumulator));
        }
        return new WindowAnalysisResult(alerts, historyUpdate);
    }

    public long getRowCount() { return rowCount; }
    public long getOffHoursRowCount() { return offHoursRowCount; }
    public int getLargeContextCount() { return largeAccumulator == null ? 0 : largeAccumulator.getContextCount(); }
}
