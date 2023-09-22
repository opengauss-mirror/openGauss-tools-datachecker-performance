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

package org.opengauss.datachecker.check.modules.bucket;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.LongHashFunctionWrapper;

import java.util.stream.IntStream;

/**
 * TestBucket
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/10
 * @since ：11
 */
public class TestBucket {
    private static final Logger log = LogUtils.getLogger();
    private static final LongHashFunctionWrapper HASH_UTIL = new LongHashFunctionWrapper();
    private static final int[] BUCKET_COUNT_LIMITS = new int[15];
    private static final int EMPTY_INITIAL_CAPACITY = 0;
    private static final int BUCKET_MAX_COUNT_LIMITS = 1 << 15;

    static {
        BUCKET_COUNT_LIMITS[0] = 1 << 1;
        BUCKET_COUNT_LIMITS[1] = 1 << 2;
        BUCKET_COUNT_LIMITS[2] = 1 << 3;
        BUCKET_COUNT_LIMITS[3] = 1 << 4;
        BUCKET_COUNT_LIMITS[4] = 1 << 5;
        BUCKET_COUNT_LIMITS[5] = 1 << 6;
        BUCKET_COUNT_LIMITS[6] = 1 << 7;
        BUCKET_COUNT_LIMITS[7] = 1 << 8;
        BUCKET_COUNT_LIMITS[8] = 1 << 9;
        BUCKET_COUNT_LIMITS[9] = 1 << 10;
        BUCKET_COUNT_LIMITS[10] = 1 << 11;
        BUCKET_COUNT_LIMITS[11] = 1 << 12;
        BUCKET_COUNT_LIMITS[12] = 1 << 13;
        BUCKET_COUNT_LIMITS[13] = 1 << 14;
        BUCKET_COUNT_LIMITS[14] = BUCKET_MAX_COUNT_LIMITS;
    }

    @Test
    public void test() {
        IntStream.rangeClosed(0, 14).forEach(idx -> {
            log.info("1<<" + (idx + 1) + " == " + BUCKET_COUNT_LIMITS[idx]);
        });
    }

    @Test
    public void test2() {
        final int limit = BUCKET_COUNT_LIMITS[6];
        IntStream.rangeClosed(0, 14).forEach(idx -> {
            final String squeueID = IdGenerator.nextId("F");
            final long hashVal = HASH_UTIL.hashBytes(squeueID);
            log.info("squeueID[{}] % limit[{}] calacA={}, calacB={}", hashVal, limit, calacA(hashVal, limit),
                calacB(hashVal, limit));
        });
    }

    private int calacA(long primaryKeyHash, int bucketCount) {
        return (int) (Math.abs(primaryKeyHash) % bucketCount);
    }

    private int calacA2(long primaryKeyHash, int bucketCount) {
        return (int) (primaryKeyHash & (bucketCount - 1));
    }

    private int calacB(long primaryKeyHash, int bucketCount) {
        return (int) (Math.abs(primaryKeyHash) & (bucketCount - 1));
    }
}
