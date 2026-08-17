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
}
