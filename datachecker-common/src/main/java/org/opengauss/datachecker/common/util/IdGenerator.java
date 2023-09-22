/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.common.util;

import org.apache.logging.log4j.Logger;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Locale;

/**
 * <pre>
 * Description: self growing ID snowflake snowflake algorithm java implementation
 * The core code is implemented by its AsSnowflakeIdGenerator class. Its principle structure is as follows.
 * I use a 0 to represent a bit, and use - to separate the functions of parts:
 * 1||0---0000000000 0000000000 0000000000 0000000000 0 --- 00000 ---00000 ---000000000000
 * In the above string, the first bit is unused (in fact, it can also be used as the symbol bit of long),
 * the next 41 bits are millisecond time, then 5 bits of data service code bit,
 * 5 bits of machine ID (not an identifier, but actually a thread identification),
 * and then 12 bits of the current millisecond count within this millisecond,
 * which adds up to just 64 bits, which is a long type.
 * The advantage of this is that on the whole, it is sorted according to the time increment,
 * and there is no ID collision in the whole distributed system (distinguished by data service and machine ID),
 * and the efficiency is high. After testing, snowflake can generate about 260000 IDS per second,
 * which fully meets the needs.
 *
 * 64 bit ID (42 (MS) +5 (machine ID) +5 (service code) +12 (repeated accumulation))
 * </pre>
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class IdGenerator {
    private static final Logger log = LogUtils.getLogger();
    /**
     * Get ID auto increment sequence
     *
     * @return Return to next ID
     */
    public static long nextId() {
        return AsSnowflakeIdGenerator.nextId();
    }

    /**
     * 36 hexadecimal string ID
     *
     * @return 36 hexadecimal string ID
     */
    public static String nextId36() {
        return Long.toString(AsSnowflakeIdGenerator.nextId(), Character.MAX_RADIX).toUpperCase(Locale.ROOT);
    }

    /**
     * Get the autoincrement sequence with the specified prefix
     *
     * @param prefix Self increasing sequence prefix
     * @return Next string ID with the specified prefix
     */
    public static String nextId(String prefix) {
        return prefix + AsSnowflakeIdGenerator.nextId();
    }

    /**
     * Gets the hexadecimal autoincrement sequence with the specified prefix
     *
     * @param prefix Self increasing sequence prefix
     * @return Next string ID with the specified prefix
     */
    public static String nextId36(String prefix) {
        return prefix + nextId36();
    }

    private static class AsSnowflakeIdGenerator {
        /**
         * The starting mark point of time, as the benchmark,
         * generally takes the latest time of the system (once it is determined, it cannot be changed)
         */
        private static final long BENCHMARK = 1653824897654L;

        /**
         * Machine identification digit
         */
        private static final long MACHINE_ID_BITS = 5L;

        /**
         * Data service identification digit
         */
        private static final long DATA_SERVICE_ID_BITS = 5L;

        /**
         * Machine ID Max 31
         */
        private static final long MAX_WORKER_MACHINE_ID = ~(-1L << MACHINE_ID_BITS);

        /**
         * Maximum data service ID 31
         */
        private static final long MAX_DATA_SERVICE_ID = ~(-1L << DATA_SERVICE_ID_BITS);

        /**
         * Self increment in milliseconds
         */
        private static final long SELF_INCREMENT_SEQUENCE_BITS = 12L;

        /**
         * Machine ID shifts 12 bits to the left
         */
        private static final long WORKER_ID_SHIFT = SELF_INCREMENT_SEQUENCE_BITS;

        /**
         * Data service ID shifts 17 bits left
         */
        private static final long DATA_SERVICE_ID_SHIFT = SELF_INCREMENT_SEQUENCE_BITS + MACHINE_ID_BITS;

        /**
         * Shift 22 bits left in milliseconds
         */
        private static final long TIMESTAMP_LEFT_SHIFT =
            SELF_INCREMENT_SEQUENCE_BITS + MACHINE_ID_BITS + DATA_SERVICE_ID_BITS;

        /**
         * max self increment sequence is 4095}
         */
        private static final long SEQUENCE_MASK = ~(-1L << SELF_INCREMENT_SEQUENCE_BITS);

        /**
         * single instance
         */
        private static final AsSnowflakeIdGenerator ID_GENERATOR = new AsSnowflakeIdGenerator();

        /**
         * Last production ID timestamp
         */
        private static long lastTimeMillis = -1L;

        private final long generatorId;

        /**
         * Data identification ID part
         */
        private final long dataServiceId;

        /**
         * 0，Concurrency control
         */
        private long sequence = 0L;

        private AsSnowflakeIdGenerator() {
            dataServiceId = getDataServiceId();
            generatorId = getMaxGeneratorId(dataServiceId);
        }

        /**
         * Get ID auto increment sequence
         *
         * @return Return to next ID
         */
        public static long nextId() {
            return ID_GENERATOR.next();
        }

        private long getDataServiceId() {
            long serviceId = 0L;
            try {
                NetworkInterface network = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
                if (network == null || network.getHardwareAddress() == null) {
                    serviceId = 1L;
                } else {
                    byte[] macAddress = network.getHardwareAddress();
                    serviceId = ((0x000000FF & (long) macAddress[macAddress.length - 1]) | (0x0000FF00 & (
                        ((long) macAddress[macAddress.length - 2]) << 8))) >> 6;
                    serviceId = serviceId % (MAX_DATA_SERVICE_ID + 1);
                }
            } catch (SocketException | UnknownHostException e) {
                log.error(" getDataServiceId: {}", e.getMessage());
            }
            return serviceId;
        }

        private long getMaxGeneratorId(long dataServiceId) {
            StringBuffer jvmPid = new StringBuffer();
            jvmPid.append(dataServiceId);
            String jvmName = ManagementFactory.getRuntimeMXBean().getName();
            if (!jvmName.isEmpty()) {
                jvmPid.append(jvmName.split("@")[0]);
            }
            return (jvmPid.toString().hashCode() & 0xffff) % (MAX_WORKER_MACHINE_ID + 1);
        }

        /**
         * Get next ID
         *
         * @return Return to next ID
         */
        private synchronized long next() {
            long currentTimestamp = currentTimeMillis();
            if (currentTimestamp < lastTimeMillis) {
                throw new ClockMovedBackwardException(String
                    .format(Locale.ROOT, "Clock moved backwards.  Refusing to generate id for %d milliseconds",
                        lastTimeMillis - currentTimestamp));
            }

            if (lastTimeMillis == currentTimestamp) {
                sequence = (sequence + 1) & SEQUENCE_MASK;
                if (sequence == 0) {
                    currentTimestamp = nextMillis(lastTimeMillis);
                }
            } else {
                sequence = 0L;
            }
            lastTimeMillis = currentTimestamp;
            return ((currentTimestamp - BENCHMARK) << TIMESTAMP_LEFT_SHIFT) | (dataServiceId << DATA_SERVICE_ID_SHIFT)
                | (generatorId << WORKER_ID_SHIFT) | sequence;
        }

        private long nextMillis(final long lastTimeMillis) {
            long timeMillis = currentTimeMillis();
            while (timeMillis <= lastTimeMillis) {
                timeMillis = currentTimeMillis();
            }
            return timeMillis;
        }

        private long currentTimeMillis() {
            return System.currentTimeMillis();
        }
    }

    static class ClockMovedBackwardException extends RuntimeException {
        private static final long serialVersionUID = -382053228395414722L;

        public ClockMovedBackwardException(String message) {
            super(message);
        }
    }
}
