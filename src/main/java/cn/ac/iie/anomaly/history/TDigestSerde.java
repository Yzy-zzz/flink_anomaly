package cn.ac.iie.anomaly.history;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

import java.nio.ByteBuffer;
import java.util.Arrays;

/** Compact serialization helpers for t-digest checkpoint state. */
final class TDigestSerde {
    private TDigestSerde() {
    }

    static byte[] encode(TDigest digest) {
        ByteBuffer buffer = ByteBuffer.allocate(digest.smallByteSize());
        digest.asSmallBytes(buffer);
        return Arrays.copyOf(buffer.array(), buffer.position());
    }

    static TDigest decode(byte[] bytes) {
        return MergingDigest.fromBytes(ByteBuffer.wrap(bytes));
    }
}
