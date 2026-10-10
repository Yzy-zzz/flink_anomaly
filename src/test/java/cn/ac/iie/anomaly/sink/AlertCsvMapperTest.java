package cn.ac.iie.anomaly.sink;

import cn.ac.iie.anomaly.model.AlertRecord;
import cn.ac.iie.anomaly.model.MetricRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class AlertCsvMapperTest {
    @Test
    public void writesValuesInFixedOrderWithOneJsonDetailColumn() throws Exception {
        AlertRecord alert = offHours("", "", "V001");
        String csv = new AlertCsvMapper().map(alert);
        Assert.assertEquals("2026100912345600000000000000000001|2026-10-09 21:00:00"
                + "|10.0.0.1|8.8.8.8|TLS|12450|15728640|3"
                + "|\"{\"\"topRank\"\":1,\"\"TopN\"\":100,\"\"conns\"\":50}\"|||V001", csv);
        Assert.assertEquals(12, csv.split("\\|", -1).length);
    }

    @Test
    public void preservesLargeTrafficEvidenceAsAJsonObject() throws Exception {
        MetricRecord metric = new MetricRecord("2026-10-09 21:00:00", 0L,
                "10.0.0.1", "8.8.8.8", "TCP", 1L, 3L, 5L, 100L, 200L);
        AlertRecord alert = AlertRecord.largeTraffic("V001", "", "", metric,
                new AlertRecord.LargeTrafficEvidence(20L, 1L, 80d, 4d, 160d, true));
        alert.assignLogId("2026100912345600000000000000000001");
        String[] fields = new AlertCsvMapper().map(alert).split("\\|", -1);
        Assert.assertEquals("8", fields[5]);
        Assert.assertEquals("300", fields[6]);
        Assert.assertEquals("2", fields[7]);
        String detailJson = fields[8].substring(1, fields[8].length() - 1).replace("\"\"", "\"");
        ObjectMapper mapper = new ObjectMapper();
        Assert.assertEquals(mapper.readTree(mapper.writeValueAsString(alert.getAnomalyDetail())),
                mapper.readTree(detailJson));
    }

    @Test
    public void quotesDelimitersQuotesAndNewlinesAndKeepsUtf8() throws Exception {
        AlertRecord alert = offHours("备注|\"内容\"\r\n下一行", null, "厂商");
        String csv = new AlertCsvMapper().map(alert);
        Assert.assertTrue(csv.endsWith("|\"备注|\"\"内容\"\"\r\n下一行\"||厂商"));
        Assert.assertArrayEquals(csv.getBytes(StandardCharsets.UTF_8),
                new SimpleStringSchema(StandardCharsets.UTF_8).serialize(csv));
    }

    @Test
    public void keepsNullAndWhitespaceOnlyFieldsEmptyIncludingTrailingColumns() throws Exception {
        AlertRecord alert = offHours(null, "   ", null);
        String csv = new AlertCsvMapper().map(alert);
        Assert.assertTrue(csv.endsWith("|||"));
        String[] fields = csv.split("\\|", -1);
        Assert.assertEquals(12, fields.length);
        Assert.assertEquals("", fields[9]);
        Assert.assertEquals("", fields[10]);
        Assert.assertEquals("", fields[11]);
        Assert.assertFalse(csv.contains("null"));
    }

    private static AlertRecord offHours(String remark1, String remark2, String vendorCode) {
        AlertRecord alert = AlertRecord.offHours(vendorCode, remark1, remark2,
                "2026-10-09 21:00:00", "10.0.0.1", "8.8.8.8", "TLS", 1, 100, 50L);
        alert.assignLogId("2026100912345600000000000000000001");
        alert.assignTrafficTotals(12450L, 15728640L);
        return alert;
    }
}
