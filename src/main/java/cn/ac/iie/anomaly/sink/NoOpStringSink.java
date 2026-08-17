package cn.ac.iie.anomaly.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class NoOpStringSink extends RichSinkFunction<String> {
    private static final long serialVersionUID = 1L;
    @Override
    public void invoke(String value, Context context) {
        // Intentionally discard alerts when all external outputs are disabled.
    }
}
