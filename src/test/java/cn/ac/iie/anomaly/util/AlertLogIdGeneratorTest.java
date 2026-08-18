package cn.ac.iie.anomaly.util;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.ZoneId;

public class AlertLogIdGeneratorTest {

    @Test
    public void firstIdsUseEighteenDigitZeroPaddedSequence() {
        AlertLogIdGenerator generator = new AlertLogIdGenerator("123456", ZoneId.of("Asia/Shanghai"));
        LocalDate date = LocalDate.of(2026, 8, 18);

        String first = generator.nextId(date);
        String second = generator.nextId(date);

        Assert.assertEquals("20260818123456000000000000000000", first);
        Assert.assertEquals("20260818123456000000000000000001", second);
        Assert.assertEquals(32, first.length());
        Assert.assertTrue(first.matches("\\d{32}"));
    }

    @Test
    public void sequenceResetsToZeroOnNextDay() {
        AlertLogIdGenerator generator = new AlertLogIdGenerator("654321", ZoneId.of("Asia/Shanghai"));
        generator.nextId(LocalDate.of(2026, 8, 18));
        generator.nextId(LocalDate.of(2026, 8, 18));

        String nextDayFirst = generator.nextId(LocalDate.of(2026, 8, 19));

        Assert.assertEquals("20260819654321000000000000000000", nextDayFirst);
        Assert.assertEquals(1L, generator.getNextSequence());
    }

    @Test
    public void checkpointStateRestoresNextSequence() {
        AlertLogIdGenerator original = new AlertLogIdGenerator("123456", ZoneId.of("Asia/Shanghai"));
        LocalDate date = LocalDate.of(2026, 8, 18);
        original.nextId(date);
        original.nextId(date);
        String snapshot = original.snapshotState();

        AlertLogIdGenerator restored = new AlertLogIdGenerator("123456", ZoneId.of("Asia/Shanghai"));
        restored.restoreState(snapshot);

        Assert.assertEquals("20260818123456000000000000000002", restored.nextId(date));
    }

    @Test(expected = IllegalArgumentException.class)
    public void deviceIdMustBeExactlySixDigits() {
        new AlertLogIdGenerator("12345", ZoneId.of("Asia/Shanghai"));
    }

    @Test(expected = IllegalStateException.class)
    public void backwardsSystemDateIsRejectedToProtectUniqueness() {
        AlertLogIdGenerator generator = new AlertLogIdGenerator("123456", ZoneId.of("Asia/Shanghai"));
        generator.nextId(LocalDate.of(2026, 8, 18));
        generator.nextId(LocalDate.of(2026, 8, 17));
    }
}
