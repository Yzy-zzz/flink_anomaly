package cn.ac.iie.anomaly.util;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * 事件记录本地 ID 生成器。
 *
 * 格式固定为：yyyyMMdd + 6位设备ID + 18位日内自增序号，共 32 位数字。
 * 例如设备 ID=123456，当天第一条告警为：
 * 20260818123456000000000000000000
 *
 * 序号每天从 0 开始。Source 会把本生成器的日期和 nextSequence 一起写入
 * Flink Operator State，因此正常 Checkpoint/恢复后可以从已保存的位置继续。
 */
public final class AlertLogIdGenerator implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int DEVICE_ID_WIDTH = 6;
    public static final int SEQUENCE_WIDTH = 18;
    public static final int ID_WIDTH = 8 + DEVICE_ID_WIDTH + SEQUENCE_WIDTH;
    public static final long MAX_SEQUENCE = 999_999_999_999_999_999L;

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.BASIC_ISO_DATE;
    private static final String STATE_SEPARATOR = "|";

    private final String deviceId;
    private final ZoneId zoneId;

    /** 当前序号所属的系统日期。 */
    private LocalDate sequenceDate;
    /** 下一条告警要使用的序号；当天第一条为 0。 */
    private long nextSequence;

    public AlertLogIdGenerator(String deviceId, ZoneId zoneId) {
        if (deviceId == null || !deviceId.matches("\\d{" + DEVICE_ID_WIDTH + "}")) {
            throw new IllegalArgumentException("alert.device.id must be exactly 6 digits");
        }
        if (zoneId == null) {
            throw new IllegalArgumentException("zoneId must not be null");
        }
        this.deviceId = deviceId;
        this.zoneId = zoneId;
    }

    /** 按配置时区的当前系统日期生成下一条 ID。 */
    public String nextId() {
        return nextId(LocalDate.now(zoneId));
    }

    /**
     * 指定日期生成下一条 ID。公开此重载主要用于确定性的单元测试。
     * 生产代码使用 {@link #nextId()}。
     */
    public String nextId(LocalDate currentDate) {
        if (currentDate == null) {
            throw new IllegalArgumentException("currentDate must not be null");
        }

        if (sequenceDate == null) {
            sequenceDate = currentDate;
            nextSequence = 0L;
        } else if (currentDate.isAfter(sequenceDate)) {
            // 新的一天从 0 开始。
            sequenceDate = currentDate;
            nextSequence = 0L;
        } else if (currentDate.isBefore(sequenceDate)) {
            // 宁可停止，也不在系统时钟跨日回拨时重新使用旧日期的序号。
            throw new IllegalStateException(
                    "System date moved backwards from " + sequenceDate + " to " + currentDate
                            + "; refusing to generate a potentially duplicate logId");
        }

        if (nextSequence < 0L || nextSequence > MAX_SEQUENCE) {
            throw new IllegalStateException(
                    "Daily alert sequence exhausted for " + sequenceDate
                            + "; max sequence is " + MAX_SEQUENCE);
        }

        long sequence = nextSequence++;
        String sequenceText = Long.toString(sequence);
        StringBuilder id = new StringBuilder(ID_WIDTH);
        id.append(sequenceDate.format(DATE_FORMAT));
        id.append(deviceId);
        for (int i = sequenceText.length(); i < SEQUENCE_WIDTH; i++) {
            id.append('0');
        }
        id.append(sequenceText);
        return id.toString();
    }

    /**
     * 生成可直接放入 Flink ListState<String> 的紧凑状态。
     * 空字符串表示还没有生成过任何 ID。
     */
    public String snapshotState() {
        if (sequenceDate == null) {
            return "";
        }
        return sequenceDate.format(DATE_FORMAT) + STATE_SEPARATOR + nextSequence;
    }

    /** 从 Checkpoint 恢复“日期 + 下一序号”。 */
    public void restoreState(String state) {
        if (state == null || state.trim().isEmpty()) {
            return;
        }
        String value = state.trim();
        int separator = value.indexOf(STATE_SEPARATOR);
        if (separator <= 0 || separator == value.length() - 1) {
            throw new IllegalArgumentException("Invalid alert logId generator state: " + value);
        }

        LocalDate restoredDate = LocalDate.parse(value.substring(0, separator), DATE_FORMAT);
        long restoredNextSequence = Long.parseLong(value.substring(separator + 1));
        if (restoredNextSequence < 0L || restoredNextSequence > MAX_SEQUENCE + 1L) {
            throw new IllegalArgumentException(
                    "Invalid restored alert nextSequence: " + restoredNextSequence);
        }
        this.sequenceDate = restoredDate;
        this.nextSequence = restoredNextSequence;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public LocalDate getSequenceDate() {
        return sequenceDate;
    }

    public long getNextSequence() {
        return nextSequence;
    }
}
