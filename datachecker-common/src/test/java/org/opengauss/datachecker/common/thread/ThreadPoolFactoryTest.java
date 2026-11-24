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

package org.opengauss.datachecker.common.thread;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(PowerMockRunner.class)
class ThreadPoolFactoryTest {
    @DisplayName("new thread pool  queue size =-1")
    @Test
    void testNewThreadPool_queue_litter_0() {
        // Run the test
        final ExecutorService executorService = ThreadPoolFactory.newThreadPool("threadName", -1);
        // Verify the results\
        assertThat(executorService.isShutdown()).isFalse();
        executorService.shutdown();
        assertThat(executorService.isShutdown()).isTrue();
    }

    @DisplayName("new thread pool  queue size =0")
    @Test
    void testNewThreadPool_queue_0() {
        // Run the test
        final ExecutorService executorService = ThreadPoolFactory.newThreadPool("threadName", 0);
        // Verify the results\
        assertThat(executorService.isShutdown()).isFalse();
        executorService.shutdown();
        assertThat(executorService.isShutdown()).isTrue();
    }

    @DisplayName("new thread pool  queue size =1")
    @Test
    void testNewThreadPool_queue_1() {
        // Run the test
        final ExecutorService executorService = ThreadPoolFactory.newThreadPool("threadName", 1);
        // Verify the results\
        assertThat(executorService.isShutdown()).isFalse();
        executorService.shutdown();
        assertThat(executorService.isShutdown()).isTrue();
    }

    @DisplayName("new thread pool queue size =100")
    @Test
    void testNewThreadPool_queue_100() {
        // Run the test
        final ExecutorService executorService = ThreadPoolFactory.newThreadPool("threadName", 100);
        // Verify the results\
        assertThat(executorService.isShutdown()).isFalse();
        executorService.shutdown();
        assertThat(executorService.isShutdown()).isTrue();
    }
}
