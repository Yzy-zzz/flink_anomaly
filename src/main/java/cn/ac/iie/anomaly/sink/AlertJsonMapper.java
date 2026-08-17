package cn.ac.iie.anomaly.sink;

import cn.ac.iie.anomaly.model.AlertRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class AlertJsonMapper implements MapFunction<AlertRecord, String> {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String map(AlertRecord value) throws JsonProcessingException {
        return MAPPER.writeValueAsString(value);
    }
}
