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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * ThreadUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/31
 * @since ：11
 */
@Slf4j
public class ThreadUtil {

    /**
     * Thread hibernation
     *
     * @param millisTime Sleep time MS
     */
    public static void sleep(int millisTime) {
        try {
            Thread.sleep(millisTime);
        } catch (InterruptedException ie) {
            log.error("thread sleep interrupted exception ");
        }
    }

    /**
     * The current thread sleeps for 10 - 500 milliseconds
     */
    public static void requestConflictingSleeping() {
        try {
            Thread.sleep(RandomUtils.nextLong(50, 500));
        } catch (InterruptedException ie) {
            log.error("thread sleep interrupted exception ");
        }
    }

    /**
     * The current thread sleeps for 1000 milliseconds
     */
    public static void sleepOneSecond() {
        sleep(1000);
    }

    /**
     * The current thread sleeps for 500 milliseconds
     */
    public static void sleepHalfSecond() {
        sleep(500);
    }

    /**
     * Custom thread pool construction
     *
     * @return thread pool
     */
    @SuppressWarnings({"all"})
    public static ExecutorService newSingleThreadExecutor() {
        return Executors.newFixedThreadPool(1, Executors.defaultThreadFactory());
    }

    /**
     * Custom scheduled task single thread
     *
     * @return Scheduled task single thread
     */
    public static ScheduledExecutorService newSingleThreadScheduledExecutor() {
        return new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder().daemon(true).build());
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String name) {
        return new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.
            Builder().namingPattern(name).build());
    }

}
