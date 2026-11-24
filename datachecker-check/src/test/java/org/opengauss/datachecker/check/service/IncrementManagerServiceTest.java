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

package org.opengauss.datachecker.check.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.check.DataCheckService;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.util.ReflectUtil;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IncrementManagerServiceTest {

    @Mock
    private DataCheckService mockDataCheckService;
    @Mock
    private ThreadPoolTaskExecutor mockAsyncCheckExecutor;
    @Mock
    private FeignClientService mockFeignClientService;

    @InjectMocks
    private IncrementManagerService incrementManagerServiceUnderTest;

    @DisplayName("testNotifySourceIncrementDataLogs one record ")
    @Test
    void testNotifySourceIncrementDataLogs() {
        // Setup
        final SourceDataLog dataLog = new SourceDataLog();
        dataLog.setTableName("tableName");
        dataLog.setBeginOffset(0L);
        dataLog.setCompositePrimarys(List.of("value"));
        dataLog.setCompositePrimaryValues(List.of("value"));
        final List<SourceDataLog> dataLogList = List.of(dataLog);
        // Run the test
        incrementManagerServiceUnderTest.notifySourceIncrementDataLogs(dataLogList);
        // Verify the results
        final LinkedBlockingQueue inc_log_queue = ReflectUtil
            .getField(IncrementManagerService.class, incrementManagerServiceUnderTest, LinkedBlockingQueue.class,
                "INC_LOG_QUEUE");
        Assert.notNull(inc_log_queue, "inc_log_queue is null");
        assertThat(inc_log_queue.size()).isEqualTo(1);
        inc_log_queue.clear();
    }

    @DisplayName("testNotifySourceIncrementDataLogs empty ")
    @Test
    void testNotifySourceIncrementDataLogs_empty() {
        // Setup

        final List<SourceDataLog> dataLogList = List.of();
        // Run the test
        incrementManagerServiceUnderTest.notifySourceIncrementDataLogs(dataLogList);
        // Verify the results
        final LinkedBlockingQueue inc_log_queue = ReflectUtil
            .getField(IncrementManagerService.class, incrementManagerServiceUnderTest, LinkedBlockingQueue.class,
                "INC_LOG_QUEUE");
        Assert.notNull(inc_log_queue, "inc_log_queue is null");
        assertThat(inc_log_queue.size()).isEqualTo(0);
    }

    @DisplayName("testNotifySourceIncrementDataLogs null")
    @Test
    void testNotifySourceIncrementDataLogs_null() {
        // Setup

        final List<SourceDataLog> dataLogList = null;
        // Run the test
        incrementManagerServiceUnderTest.notifySourceIncrementDataLogs(dataLogList);
        // Verify the results
        final LinkedBlockingQueue inc_log_queue = ReflectUtil
            .getField(IncrementManagerService.class, incrementManagerServiceUnderTest, LinkedBlockingQueue.class,
                "INC_LOG_QUEUE");
        Assert.notNull(inc_log_queue, "inc_log_queue is null");
        assertThat(inc_log_queue.size()).isEqualTo(0);
    }

    @DisplayName("startIncrementMonitor false")
    @Test
    void testStartIncrementDataLogs_start_failed() {
        // Setup
        when(mockFeignClientService.startIncrementMonitor()).thenReturn(false);
        assertThatThrownBy(() -> incrementManagerServiceUnderTest.startIncrementDataLogs())
            .isInstanceOf(CheckingException.class);
    }

    @DisplayName("startIncrementMonitor true")
    @Test
    void testStartIncrementDataLogs() {
        // Setup
        when(mockFeignClientService.startIncrementMonitor()).thenReturn(true);
        // Run the test
        incrementManagerServiceUnderTest.startIncrementDataLogs();
    }
}
