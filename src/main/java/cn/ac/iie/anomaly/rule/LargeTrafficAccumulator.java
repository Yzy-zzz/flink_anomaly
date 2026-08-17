package cn.ac.iie.anomaly.rule;

import cn.ac.iie.anomaly.model.MetricRecord;
import cn.ac.iie.anomaly.sketch.CountMinSketch;
import com.tdunning.math.stats.TDigest;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class LargeTrafficAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private final TDigest bytesDigest;
    private final TDigest pktsDigest;
    private final CountMinSketch byteSumSketch;
    private final CountMinSketch pktSumSketch;
    private final CountMinSketch countSketch;
    private final int candidateCapacity;
    private final PriorityQueue<MetricRecord> candidates;
    private long rowCount;

    public LargeTrafficAccumulator(double compression, int cmsDepth, int cmsWidth, int candidateCapacity) {
        this.bytesDigest = TDigest.createMergingDigest(compression);
        this.pktsDigest = TDigest.createMergingDigest(compression);
        this.byteSumSketch = new CountMinSketch(cmsDepth, cmsWidth);
        this.pktSumSketch = new CountMinSketch(cmsDepth, cmsWidth);
        this.countSketch = new CountMinSketch(cmsDepth, cmsWidth);
        this.candidateCapacity = candidateCapacity;
        this.candidates = new PriorityQueue<>(candidateCapacity, new MetricBytesComparator());
    }

    public void add(MetricRecord record) {
        long bytes = record.totalBytes();
        long pkts = record.totalPkts();
        String key = record.pairKey();

        bytesDigest.add(bytes);
        pktsDigest.add(pkts);
        byteSumSketch.add(key, bytes);
        pktSumSketch.add(key, pkts);
        countSketch.add(key, 1L);
        rowCount++;

        if (candidateCapacity <= 0) {
            return;
        }
        if (candidates.size() < candidateCapacity) {
            candidates.offer(record);
        } else if (bytes > candidates.peek().totalBytes()) {
            candidates.poll();
            candidates.offer(record);
        }
    }

    public void mergeFrom(LargeTrafficAccumulator other) {
        bytesDigest.add(other.bytesDigest);
        pktsDigest.add(other.pktsDigest);
        byteSumSketch.mergeFrom(other.byteSumSketch);
        pktSumSketch.mergeFrom(other.pktSumSketch);
        countSketch.mergeFrom(other.countSketch);
        rowCount += other.rowCount;
        for (MetricRecord candidate : other.candidates) {
            if (candidates.size() < candidateCapacity) {
                candidates.offer(candidate);
            } else if (candidate.totalBytes() > candidates.peek().totalBytes()) {
                candidates.poll();
                candidates.offer(candidate);
            }
        }
    }

    public TDigest getBytesDigest() { return bytesDigest; }
    public TDigest getPktsDigest() { return pktsDigest; }
    public CountMinSketch getByteSumSketch() { return byteSumSketch; }
    public CountMinSketch getPktSumSketch() { return pktSumSketch; }
    public CountMinSketch getCountSketch() { return countSketch; }
    public long getRowCount() { return rowCount; }

    public List<MetricRecord> candidateSnapshot() {
        return new ArrayList<>(candidates);
    }

    private static final class MetricBytesComparator implements Comparator<MetricRecord>, Serializable {
        private static final long serialVersionUID = 1L;
        @Override
        public int compare(MetricRecord left, MetricRecord right) {
            return Long.compare(left.totalBytes(), right.totalBytes());
        }
    }
}
