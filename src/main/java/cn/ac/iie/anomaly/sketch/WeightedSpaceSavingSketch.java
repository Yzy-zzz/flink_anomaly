package cn.ac.iie.anomaly.sketch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Weighted Space-Saving heavy-hitter sketch with a bounded number of counters.
 * Update complexity is O(log capacity); memory is O(capacity).
 */
public class WeightedSpaceSavingSketch implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int capacity;
    private final ArrayList<Entry> heap;
    private final Map<String, Integer> indexByKey;

    public WeightedSpaceSavingSketch(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
        this.heap = new ArrayList<>(capacity);
        this.indexByKey = new HashMap<>(capacity * 2);
    }

    public void update(String key, long weight, String collectTime) {
        if (key == null || weight <= 0) {
            return;
        }
        Integer index = indexByKey.get(key);
        if (index != null) {
            Entry entry = heap.get(index);
            entry.estimate = saturatingAdd(entry.estimate, weight);
            entry.collectTime = laterCollectTime(entry.collectTime, collectTime);
            siftDown(index);
            return;
        }

        if (heap.size() < capacity) {
            Entry entry = new Entry(key, weight, 0L, collectTime);
            heap.add(entry);
            int newIndex = heap.size() - 1;
            indexByKey.put(key, newIndex);
            siftUp(newIndex);
            return;
        }

        Entry min = heap.get(0);
        indexByKey.remove(min.key);
        long oldMin = min.estimate;
        min.key = key;
        min.error = oldMin;
        min.estimate = saturatingAdd(oldMin, weight);
        min.collectTime = collectTime;
        indexByKey.put(key, 0);
        siftDown(0);
    }

    public List<Entry> topN(int n) {
        ArrayList<Entry> copy = new ArrayList<>(heap.size());
        for (Entry entry : heap) {
            copy.add(entry.copy());
        }
        copy.sort(Comparator.comparingLong(Entry::getEstimate).reversed());
        if (copy.size() > n) {
            return new ArrayList<>(copy.subList(0, n));
        }
        return copy;
    }

    public List<Entry> entries() {
        ArrayList<Entry> copy = new ArrayList<>(heap.size());
        for (Entry entry : heap) {
            copy.add(entry.copy());
        }
        return Collections.unmodifiableList(copy);
    }

    public void mergeFrom(WeightedSpaceSavingSketch other) {
        for (Entry entry : other.entries()) {
            update(entry.key, entry.estimate, entry.collectTime);
        }
    }

    public int size() {
        return heap.size();
    }

    private void siftUp(int index) {
        int current = index;
        while (current > 0) {
            int parent = (current - 1) >>> 1;
            if (heap.get(parent).estimate <= heap.get(current).estimate) {
                break;
            }
            swap(parent, current);
            current = parent;
        }
    }

    private void siftDown(int index) {
        int current = index;
        int size = heap.size();
        while (true) {
            int left = (current << 1) + 1;
            if (left >= size) {
                break;
            }
            int right = left + 1;
            int smallest = left;
            if (right < size && heap.get(right).estimate < heap.get(left).estimate) {
                smallest = right;
            }
            if (heap.get(current).estimate <= heap.get(smallest).estimate) {
                break;
            }
            swap(current, smallest);
            current = smallest;
        }
    }

    private void swap(int i, int j) {
        Entry a = heap.get(i);
        Entry b = heap.get(j);
        heap.set(i, b);
        heap.set(j, a);
        indexByKey.put(a.key, j);
        indexByKey.put(b.key, i);
    }


    private static String laterCollectTime(String left, String right) {
        if (left == null || left.isEmpty()) {
            return right;
        }
        if (right == null || right.isEmpty()) {
            return left;
        }
        // collectTime is normalized to yyyy-MM-dd HH:mm:ss, so lexical order is time order.
        return left.compareTo(right) >= 0 ? left : right;
    }

    private static long saturatingAdd(long a, long b) {
        if (b > 0 && a > Long.MAX_VALUE - b) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    public static class Entry implements Serializable {
        private static final long serialVersionUID = 1L;
        private String key;
        private long estimate;
        private long error;
        private String collectTime;

        Entry(String key, long estimate, long error, String collectTime) {
            this.key = key;
            this.estimate = estimate;
            this.error = error;
            this.collectTime = collectTime;
        }

        Entry copy() {
            return new Entry(key, estimate, error, collectTime);
        }

        public String getKey() { return key; }
        public long getEstimate() { return estimate; }
        public long getError() { return error; }
        public long getLowerBound() { return Math.max(0L, estimate - error); }
        public String getCollectTime() { return collectTime; }
    }
}
