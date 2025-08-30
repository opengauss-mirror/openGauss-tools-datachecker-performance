/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.common.util;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ThreadPoolShutdownUtil
 *
 * @author: wangchao
 * @Date: 2025/8/30 12:22
 * @since: 11
 **/
@Slf4j
public class ThreadPoolShutdownUtil {
    /**
     * shutdown executor service
     *
     * @param executorService executor service
     * @param executorName executor name
     */
    public static void shutdown(ExecutorService executorService, String executorName) {
        try {
            log.info("Shutting down executor service [name={}, threadCount={}, queuedTasks={}]", executorName,
                getActiveThreadCount(executorService), getQueueSize(executorService));
            executorService.shutdown();
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("executor[{}] did not terminate gracefully, forcing shutdown", executorName);
                executorService.shutdownNow();
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.error("executor[{}] did not terminate", executorName);
                }
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted during executor[{}] shutdown", executorName);
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static int getQueueSize(ExecutorService executorService) {
        if (executorService instanceof ThreadPoolExecutor executor) {
            return executor.getQueue().size();
        }
        return -1;
    }

    private static int getActiveThreadCount(ExecutorService executorService) {
        if (executorService instanceof ThreadPoolExecutor executor) {
            return executor.getActiveCount();
        }
        return -1;
    }
}
