package cn.ac.iie.anomaly.sketch;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class WeightedSpaceSavingSketchTest {
    @Test
    public void keepsHeavyHitter() {
        WeightedSpaceSavingSketch sketch = new WeightedSpaceSavingSketch(4);
        for (int i = 0; i < 50; i++) {
            sketch.update("heavy", 10L, "2026-01-01 20:00:00");
            sketch.update("light-" + i, 1L, "2026-01-01 20:00:00");
        }
        List<WeightedSpaceSavingSketch.Entry> top = sketch.topN(1);
        Assert.assertEquals(1, top.size());
        Assert.assertEquals("heavy", top.get(0).getKey());
        Assert.assertTrue(top.get(0).getEstimate() >= 500L);
    }

    @Test
    public void replacementDoesNotInheritAnotherConnectionsTraffic() {
        WeightedSpaceSavingSketch sketch = new WeightedSpaceSavingSketch(1);
        sketch.update("evicted", 2L, "2026-01-01 20:00:00", 100L, 1000L);
        sketch.update("retained", 3L, "2026-01-01 20:01:00", 4L, 40L);
        sketch.update("retained", 0L, "2026-01-01 20:02:00", 5L, 50L);
        WeightedSpaceSavingSketch.Entry entry = sketch.topN(1).get(0);
        Assert.assertEquals("retained", entry.getKey());
        Assert.assertEquals(5L, entry.getEstimate());
        Assert.assertEquals(9L, entry.getPacketCount());
        Assert.assertEquals(90L, entry.getTotalBytes());
        Assert.assertEquals("2026-01-01 20:02:00", entry.getCollectTime());
    }

    @Test
    public void mergeAndCopiesPreserveTrafficAndSaturateOnOverflow() {
        WeightedSpaceSavingSketch sketch = new WeightedSpaceSavingSketch(2);
        sketch.update("pair", 1L, "2026-01-01 20:00:00", Long.MAX_VALUE - 1L, Long.MAX_VALUE - 5L);
        WeightedSpaceSavingSketch other = new WeightedSpaceSavingSketch(2);
        other.update("pair", 2L, "2026-01-01 20:01:00", 10L, 100L);
        sketch.mergeFrom(other);
        WeightedSpaceSavingSketch.Entry entry = sketch.entries().get(0);
        Assert.assertEquals(3L, entry.getEstimate());
        Assert.assertEquals(Long.MAX_VALUE, entry.getPacketCount());
        Assert.assertEquals(Long.MAX_VALUE, entry.getTotalBytes());
        Assert.assertEquals(Long.MAX_VALUE, sketch.topN(1).get(0).getTotalBytes());
    }
}
