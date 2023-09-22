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

package org.opengauss.datachecker.check.task;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.common.util.LogUtils;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TableStatusRegisterTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/20
 * @since ：11
 */
class TableStatusRegisterTest {
    private static final Logger log = LogUtils.getLogger();
    private TableStatusRegister tableStatusRegisterUnderTest;

    @BeforeEach
    void setUp() {
        tableStatusRegisterUnderTest = new TableStatusRegister();
        testInit();
    }

    @Test
    void testInit() {
        // Setup
        // Run the test
        tableStatusRegisterUnderTest.init(Set.of("tabel1", "tabel2", "tabel3", "tabel4"));
        // Verify the results
    }

    @Test
    void testPut() {
        tableStatusRegisterUnderTest.put("tabel5", 3);
    }

    @Test
    void testGet() {
        assertThat(tableStatusRegisterUnderTest.get("tabel1")).isEqualTo(0);
    }

    @Test
    void testUpdate() {
        log.info("0|1 = " + (0 | 1));
        log.info("0|2 = " + (0 | 2));
        log.info("1|1 = " + (1 | 1));
        log.info("1|2 = " + (1 | 2));
        log.info("1|2|4 = " + (1 | 2 | 4));
        log.info("4 = " + Integer.toBinaryString(4));
        log.info("" + tableStatusRegisterUnderTest.get("tabel1"));
        assertThat(tableStatusRegisterUnderTest.update("tabel1", 1)).isEqualTo(1);
    }

    @Test
    void testRemove() {
        // Setup
        // Run the test
        tableStatusRegisterUnderTest.remove("key");
        // Verify the results
    }

    @Test
    void testRemoveAll() {
        // Setup
        // Run the test
        tableStatusRegisterUnderTest.removeAll();
        // Verify the results
    }
}
