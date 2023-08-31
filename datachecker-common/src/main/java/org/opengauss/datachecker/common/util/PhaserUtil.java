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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * PhaserUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/22
 * @since ：11
 */
public class PhaserUtil {
    private static final Logger log = LogUtils.getLogger();
    /**
     * Use the thread pool to submit parallel tasks. When all parallel tasks are completed, execute the complete task
     *
     * @param executorService executorService
     * @param tasks           tasks
     * @param complete        complete
     */
    public static void submit(ThreadPoolTaskExecutor executorService, List<Runnable> tasks, Runnable complete) {
        final List<Future<?>> futureList = new ArrayList<>();
        for (Runnable runnable : tasks) {
            futureList.add(executorService.submit(runnable::run));
        }
        getAllTaskFuture(futureList);
        complete.run();
    }

    public static void submit(ThreadPoolExecutor executorService, List<Runnable> tasks, Runnable complete) {
        final List<Future<?>> futureList = new ArrayList<>();
        for (Runnable runnable : tasks) {
            futureList.add(executorService.submit(runnable::run));
        }
        getAllTaskFuture(futureList);
        complete.run();
    }

    private static void getAllTaskFuture(List<Future<?>> futureList) {
        futureList.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("future  error: {}", e.getMessage());
            }
        });
    }

    /**
     * Check whether all thread pool tasks are completed. When all thread pool tasks are completed, close the thread pool
     *
     * @param executorService executorService
     * @param futureList      futureList
     */
    public static void executorComplete(ExecutorService executorService, List<Future<?>> futureList) {
        getAllTaskFuture(futureList);
        executorService.shutdown();
    }

}
