package cn.ac.iie.anomaly.sketch;

import java.io.Serializable;

/**
 * Fixed-memory Count-Min Sketch for non-negative long weights.
 * All estimates are upper bounds (subject to long saturation).
 */
public class CountMinSketch implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int depth;
    private final int width;
    private final long[][] table;
    private final long[] seeds;

    public CountMinSketch(int depth, int width) {
        if (depth <= 0 || width <= 0) {
            throw new IllegalArgumentException("depth and width must be positive");
        }
        this.depth = depth;
        this.width = width;
        this.table = new long[depth][width];
        this.seeds = new long[depth];
        for (int i = 0; i < depth; i++) {
            seeds[i] = mix64(0x9E3779B97F4A7C15L + i * 0xC2B2AE3D27D4EB4FL);
        }
    }

    public void add(String key, long weight) {
        if (weight <= 0) {
            return;
        }
        long base = hashString64(key);
        for (int i = 0; i < depth; i++) {
            int index = indexFor(base, seeds[i]);
            table[i][index] = saturatingAdd(table[i][index], weight);
        }
    }

    public long estimate(String key) {
        long base = hashString64(key);
        long min = Long.MAX_VALUE;
        for (int i = 0; i < depth; i++) {
            int index = indexFor(base, seeds[i]);
            min = Math.min(min, table[i][index]);
        }
        return min == Long.MAX_VALUE ? 0L : min;
    }

    public void mergeFrom(CountMinSketch other) {
        if (other.depth != depth || other.width != width) {
            throw new IllegalArgumentException("CountMinSketch shape mismatch");
        }
        for (int i = 0; i < depth; i++) {
            for (int j = 0; j < width; j++) {
                table[i][j] = saturatingAdd(table[i][j], other.table[i][j]);
            }
        }
    }

    public long estimatedBytes() {
        return (long) depth * width * Long.BYTES;
    }

    private int indexFor(long base, long seed) {
        long mixed = mix64(base ^ seed);
        return (int) Math.floorMod(mixed, (long) width);
    }

    private static long hashString64(String value) {
        String s = value == null ? "" : value;
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            hash ^= (c & 0xff);
            hash *= 0x100000001b3L;
            hash ^= ((c >>> 8) & 0xff);
            hash *= 0x100000001b3L;
        }
        return mix64(hash);
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    private static long saturatingAdd(long a, long b) {
        if (b > 0 && a > Long.MAX_VALUE - b) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }
}
