package cn.ac.iie.anomaly.source;

import cn.ac.iie.anomaly.model.MetricRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.time.ZoneId;

public class MetricJsonDecoderTest {
    private final ObjectMapper mapper = new ObjectMapper();
    private final ZoneId zone = ZoneId.of("Asia/Shanghai");

    @Test
    public void decodesProvidedMetricShape() throws Exception {
        String json = "{\"c2sBytes\":100,\"c2sPkts\":1,\"collectTime\":\"2026-08-24 13:05:00\","
                + "\"connCount\":1,\"dstIp\":\"183.93.235.217\",\"protocol\":\"UNKNOWN\","
                + "\"s2cBytes\":0,\"s2cPkts\":0,\"srcIp\":\"1.224.99.3\"}";
        MetricRecord record = MetricJsonDecoder.decode(json, mapper, zone);
        Assert.assertEquals("2026-08-24 13:05:00", record.getCollectTime());
        Assert.assertEquals(100L, record.totalBytes());
        Assert.assertEquals(1L, record.totalPkts());
        Assert.assertEquals(1L, record.getConnCount());
    }

    @Test
    public void missingAndNegativeCountersNormalizeToZero() throws Exception {
        String json = "{\"collectTime\":\"2026-08-24 13:05:00\",\"srcIp\":\"1.2.3.4\","
                + "\"dstIp\":\"5.6.7.8\",\"c2sBytes\":-5}";
        MetricRecord record = MetricJsonDecoder.decode(json, mapper, zone);
        Assert.assertEquals(0L, record.totalBytes());
        Assert.assertEquals(0L, record.totalPkts());
        Assert.assertEquals(0L, record.getConnCount());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsInvalidIp() throws Exception {
        String json = "{\"collectTime\":\"2026-08-24 13:05:00\",\"srcIp\":\"not-an-ip\","
                + "\"dstIp\":\"5.6.7.8\"}";
        MetricJsonDecoder.decode(json, mapper, zone);
    }
}
