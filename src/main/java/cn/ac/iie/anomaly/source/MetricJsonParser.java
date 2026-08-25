package cn.ac.iie.anomaly.source;

import cn.ac.iie.anomaly.config.AppConfig;
import cn.ac.iie.anomaly.model.MetricRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;

/** Parses Kafka JSON and drops malformed rows without failing the whole job. */
public final class MetricJsonParser extends RichFlatMapFunction<String, MetricRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(MetricJsonParser.class);

    private final AppConfig config;
    private transient ObjectMapper mapper;
    private transient ZoneId zoneId;
    private transient LongCounter goodRows;
    private transient LongCounter badRows;
    private long localBadRows;

    public MetricJsonParser(AppConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        this.mapper = new ObjectMapper();
        this.zoneId = ZoneId.of(config.get("business.timezone", "Asia/Shanghai"));
        this.goodRows = getRuntimeContext().getLongCounter("kafka-input-good-json-rows");
        this.badRows = getRuntimeContext().getLongCounter("kafka-input-bad-json-rows");
    }

    @Override
    public void flatMap(String value, Collector<MetricRecord> out) {
        try {
            MetricRecord record = MetricJsonDecoder.decode(value, mapper, zoneId);
            goodRows.add(1L);
            out.collect(record);
        } catch (Exception e) {
            badRows.add(1L);
            localBadRows++;
            if (localBadRows <= 20L || localBadRows % 10000L == 0L) {
                LOG.warn("Skip malformed Kafka metric JSON: badRowsInSubtask={}, error={}, payload={}",
                        localBadRows, e.toString(), abbreviate(value, 1000));
            }
        }
    }

    private static String abbreviate(String value, int max) {
        if (value == null) {
            return "null";
        }
        if (value.length() <= max) {
            return value;
        }
        return value.substring(0, max) + "...";
    }
}
