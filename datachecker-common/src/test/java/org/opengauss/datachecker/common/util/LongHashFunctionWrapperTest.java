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
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * LongHashFunctionWrapperTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
class LongHashFunctionWrapperTest {
    private static final Logger log = LogUtils.getLogger();
    private static final LongHashFunctionWrapper HASH_UTIL = new LongHashFunctionWrapper();
    private static final int[] SCOP_BUCKET_COUNT = new int[15];

    static {
        SCOP_BUCKET_COUNT[0] = 1 << 1;
        SCOP_BUCKET_COUNT[1] = 1 << 2;
        SCOP_BUCKET_COUNT[2] = 1 << 3;
        SCOP_BUCKET_COUNT[3] = 1 << 4;
        SCOP_BUCKET_COUNT[4] = 1 << 5;
        SCOP_BUCKET_COUNT[5] = 1 << 6;
        SCOP_BUCKET_COUNT[6] = 1 << 7;
        SCOP_BUCKET_COUNT[7] = 1 << 8;
        SCOP_BUCKET_COUNT[8] = 1 << 9;
        SCOP_BUCKET_COUNT[9] = 1 << 10;
        SCOP_BUCKET_COUNT[10] = 1 << 11;
        SCOP_BUCKET_COUNT[11] = 1 << 12;
        SCOP_BUCKET_COUNT[12] = 1 << 13;
        SCOP_BUCKET_COUNT[13] = 1 << 14;
        SCOP_BUCKET_COUNT[14] = 1 << 15;
    }

    @Test
    void testHashBytes1() {
        int mod = 600;
        AtomicInteger min = new AtomicInteger();
        AtomicInteger max = new AtomicInteger();
        int[] resultCount = new int[mod * 2];
        IntStream.range(1, 100000).forEach(idx -> {
            long xHash = HASH_UTIL.hashBytes(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
            int xmod = (int) (xHash % mod + mod);
            max.set(Math.max(xmod, max.get()));
            min.set(Math.min(xmod, min.get()));
            resultCount[xmod]++;
        });
        log.info("min=" + min.get() + "   max=" + max.get());
        IntStream.range(0, mod * 2).forEach(idx -> {
            log.info("idx=" + idx + "  " + resultCount[idx]);
        });
    }

    @Test
    void testHashBytes2() {
        log.info("bucket average capacity  " + 100000 / 1200);
        log.info("merkle leaf node size  " + Math.pow(2, 15));
        log.info("merkle leaf node size  " + (1 << 15));
    }

    @Test
    void testMode() {
        int mod = (int) Math.pow(2, 3);
        long xHash = HASH_UTIL.hashBytes(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        int xmod = (int) (xHash % mod + mod);
        log.info(" mod=  " + mod);
        log.info(xHash + "   " + xmod);
        log.info(xHash + "  (int) (x % mod ) =" + (int) (xHash % mod));
        log.info(xHash + " ( x & (2^n - 1) )= " + (xHash & (mod - 1)));
        log.info(xHash + "  (int) (x % mod + mod) =" + (int) (xHash % mod + mod));
        log.info(xHash + " ( x & (2^n - 1) + 2^n )= " + ((xHash & (mod - 1)) + mod));
    }

    @Test
    public void calacBucketCount() {
        int totalCount = 5;
        int bucketCount = totalCount / 5;
        log.info("" + bucketCount);
        int asInt = IntStream.range(0, 15).filter(idx -> SCOP_BUCKET_COUNT[idx] > bucketCount).findFirst().orElse(15);
        log.info("" + SCOP_BUCKET_COUNT[asInt]);
    }
}
