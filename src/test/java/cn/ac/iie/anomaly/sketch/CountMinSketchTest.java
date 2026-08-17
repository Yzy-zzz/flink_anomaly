package cn.ac.iie.anomaly.sketch;

import org.junit.Assert;
import org.junit.Test;

public class CountMinSketchTest {
    @Test
    public void estimateNeverUnderCounts() {
        CountMinSketch cms = new CountMinSketch(5, 4096);
        cms.add("a", 10L);
        cms.add("a", 20L);
        cms.add("b", 7L);
        Assert.assertTrue(cms.estimate("a") >= 30L);
        Assert.assertTrue(cms.estimate("b") >= 7L);
    }

    @Test
    public void mergeAddsCounters() {
        CountMinSketch left = new CountMinSketch(5, 4096);
        CountMinSketch right = new CountMinSketch(5, 4096);
        left.add("x", 11L);
        right.add("x", 13L);
        left.mergeFrom(right);
        Assert.assertTrue(left.estimate("x") >= 24L);
    }
}
