/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.debezium;

import com.alibaba.fastjson.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebeziumDataHandlerTest {

    private DebeziumDataHandler debeziumDataHandlerUnderTest;

    @BeforeEach
    void setUp() {
        debeziumDataHandlerUnderTest = new DebeziumStringHandler();
    }

    @Test
    void testHandler() {
        // Setup
        final LinkedBlockingQueue<DebeziumDataBean> queue = new LinkedBlockingQueue<>(10);
        String message = TestJsonUtil.getJsonText(TestJsonUtil.KEY_DEBEZIUM_ONE_TABLE_RECORD);
        // Run the test
        debeziumDataHandlerUnderTest.handler(0L, message, queue);
        // Verify the results
        assertThat(queue.size()).isEqualTo(1);
    }

    @Test
    void testHandler_ThrowsJSONException() {
        // Setup
        final LinkedBlockingQueue<DebeziumDataBean> queue = new LinkedBlockingQueue<>(10);
        // Run the test
        assertThatThrownBy(() -> debeziumDataHandlerUnderTest.handler(0L, "message", queue))
            .isInstanceOf(JSONException.class);
    }
}
