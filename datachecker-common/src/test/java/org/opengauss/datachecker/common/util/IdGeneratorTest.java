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

/**
 * IdGeneratorTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/9
 * @since ：11
 */
public class IdGeneratorTest {
    private static final Logger log = LogUtils.getLogger();
    /**
     * Data center identification digit
     */
    private static final long DATA_SERVICE_ID_BITS = 5L;

    /**
     * Maximum data center ID
     */
    private static final long MAX_DATA_CENTER_ID = ~(-1L << DATA_SERVICE_ID_BITS);

    /**
     * Self increment in milliseconds
     */
    private static final long SELF_INCREMENT_SEQUENCE_BITS = 12L;
    private static final long SEQUENCE_MASK = ~(-1L << SELF_INCREMENT_SEQUENCE_BITS);

    @Test
    void testNextId() {
        log.info("" + IdGenerator.nextId());
        log.info("" + MAX_DATA_CENTER_ID);
        log.info("SEQUENCE_MASK = " + SEQUENCE_MASK);
    }

    @Test
    void testNextId36() {
        log.info(IdGenerator.nextId36());
    }

    @Test
    void testNextIdPrefix() {
        log.info(IdGenerator.nextId("M"));
    }

    @Test
    void testNextId36Prefix() {
        log.info(IdGenerator.nextId36("M"));
    }
}
