package cn.ac.iie.anomaly.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalLogSink extends RichSinkFunction<String> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger("ANOMALY_ALERT");

    @Override
    public void invoke(String value, Context context) {
        LOG.warn("{}", value);
    }
}
