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

package org.opengauss.datachecker.extract.cache;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.util.LogUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * TestByteXOR
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
public class TestByteXOR {
    private static final Logger log = LogUtils.getLogger();
    @Test
    public void testXOR() {
        long old = 0L;
        for (int i = 0; i < 63; i++) {
            old = byteXor(old, i);
            log.info("0 ," + i + " =" + old + " Long.toBinaryString()" + Long.toBinaryString(old));
        }
    }

    @Test
    public void testBinaryArray() {
        Map<String, Byte[]> map = new HashMap<String, Byte[]>();
        long old = 0L;
        byte byteVal = 0;
        for (int i = 0; i < 63; i++) {
            old = byteXor(old, i);
            log.info("0 ," + i + " =" + old + " Long.toBinaryString()" + Long.toBinaryString(old));
        }
    }

    @Test
    public void testIntStream() {
        IntStream.range(1, 10).forEach(idx -> {
            log.info("range " + idx);
        });
        IntStream.rangeClosed(1, 10).forEach(idx -> {
            log.info("rangeClosed " + idx);
        });

        IntStream.rangeClosed(1, 10).filter(i -> i == 6).count();
    }

    private long byteXor(long value, int index) {
        // long 64为 该方法计算后63标识符保存数据状态。
        return (value | (1L << index));
    }
}
