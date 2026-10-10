package cn.ac.iie.anomaly.sink;

import cn.ac.iie.anomaly.model.AlertRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

/** Value-only, pipe-delimited CSV; anomalyDetail remains one JSON object field. */
public class AlertCsvMapper implements MapFunction<AlertRecord, String> {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String map(AlertRecord value) throws JsonProcessingException {
        String[] fields = {
                value.getLogId(),
                value.getCollectTime(),
                value.getSrcIp(),
                value.getDstIp(),
                value.getProtocol(),
                Long.toString(value.getPacketCount()),
                Long.toString(value.getTotalBytes()),
                Integer.toString(value.getAnomalyType()),
                MAPPER.writeValueAsString(value.getAnomalyDetail()),
                value.getRemark1(),
                value.getRemark2(),
                value.getVendorCode()
        };
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                csv.append('|');
            }
            appendField(csv, fields[i]);
        }
        return csv.toString();
    }

    private static void appendField(StringBuilder csv, String value) {
        if (value == null || value.trim().isEmpty()) {
            return;
        }
        if (value.indexOf('|') >= 0 || value.indexOf('"') >= 0
                || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
            csv.append('"').append(value.replace("\"", "\"\"")).append('"');
        } else {
            csv.append(value);
        }
    }
}
