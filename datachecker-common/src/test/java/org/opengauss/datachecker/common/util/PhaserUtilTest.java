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

package org.opengauss.datachecker.common.util;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

class PhaserUtilTest {

    @Test
    void testSubmit() {
        // Setup
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(16);
        executor.setMaxPoolSize(32);
        executor.setQueueCapacity(4000);
        executor.setKeepAliveSeconds(60);
        executor.setThreadNamePrefix("extract-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        executor.initialize();

        final List<Runnable> tasks = List.of(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                Thread.sleep(1000);
            }
        });
        System.out.println("task exec start      " + LocalDateTime.now());
        // Run the test
        PhaserUtil.submit(executor, tasks, () -> {
            System.out.println("task exec completed " + LocalDateTime.now());
        });
        // Verify the results
    }
}
