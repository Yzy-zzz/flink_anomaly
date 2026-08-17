package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.history.HistoricalBaselineStore;
import cn.ac.iie.anomaly.model.AlertRecord;

import java.util.List;

/** Immutable output of one fully consumed Doris five-minute window. */
public final class WindowAnalysisResult {
    private final List<AlertRecord> alerts;
    private final HistoricalBaselineStore.WindowUpdate historyUpdate;

    public WindowAnalysisResult(List<AlertRecord> alerts, HistoricalBaselineStore.WindowUpdate historyUpdate) {
        this.alerts = alerts;
        this.historyUpdate = historyUpdate;
    }

    public List<AlertRecord> getAlerts() { return alerts; }
    public HistoricalBaselineStore.WindowUpdate getHistoryUpdate() { return historyUpdate; }
}
