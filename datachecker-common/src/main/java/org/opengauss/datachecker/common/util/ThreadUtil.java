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

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/31
 * @since ：11
 */
public class ThreadUtil {
    private static final Logger log = LogUtils.getLogger(ThreadUtil.class);

    /**
     * Thread hibernation
     *
     * @param millisTime Sleep time MS
     */
    public static void sleep(int millisTime) {
        try {
            Thread.sleep(millisTime);
        } catch (InterruptedException ie) {
            LogUtils.warn(log, "thread sleep interrupted exception ");
        }
    }

    /**
     * Thread sleep second
     *
     * @param second Sleep time
     */
    public static void sleepSecond(int second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException ie) {
            LogUtils.warn(log, "thread sleep interrupted exception ");
        }
    }

    /**
     * The current thread sleeps for 10 - 500 milliseconds
     */
    public static void requestConflictingSleeping() {
        try {
            Thread.sleep(RandomUtils.nextLong(50, 500));
        } catch (InterruptedException ie) {
            LogUtils.warn(log, "thread sleep interrupted exception ");
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
     * thread sleep for 300 - 1000 milliseconds
     */
    public static void sleepMax2Second() {
        sleep(RandomUtils.nextInt(300, 2000));
    }

    /**
     * thread sleep for 300 - 1000 milliseconds
     */
    public static void sleepMaxHalfSecond() {
        sleep(RandomUtils.nextInt(100, 500));
    }

    /**
     * kill thread by thread name
     *
     * @param name thread name
     */
    public static void killThreadByName(String name) {
        AtomicInteger threadCount = new AtomicInteger(0);
        do {
            ThreadGroup currentGroup = Thread.currentThread().getThreadGroup();
            int noThreads = currentGroup.activeCount();
            Thread[] lstThreads = new Thread[noThreads];
            currentGroup.enumerate(lstThreads);
            threadCount.set(0);
            Arrays.stream(lstThreads)
                    .filter(thread -> {
                        if (StringUtils.containsIgnoreCase(thread.getName(), name)) {
                            threadCount.incrementAndGet();
                            return true;
                        }
                        return false;
                    })
                    .forEach(thread -> {
                        if (thread.getState().equals(Thread.State.WAITING)) {
                            log.warn("thread [{}] :[{} ] has interrupted", thread.getName(), thread.getState());
                            thread.interrupt();
                        } else {
                            threadCount.decrementAndGet();
                            log.warn("thread [{}] :[{} ]  has stop", thread.getName(), thread.getState());
                            thread.stop();
                        }
                    });
        } while (threadCount.get() > 0);

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
        return new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder().daemon(true)
                .build());
    }

    public static ScheduledExecutorService newSingleThreadScheduledExecutor(String name) {
        return new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.
                Builder().namingPattern(name)
                .build());
    }

}
