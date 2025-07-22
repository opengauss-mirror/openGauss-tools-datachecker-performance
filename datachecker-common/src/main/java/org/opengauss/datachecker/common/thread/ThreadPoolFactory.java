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

package org.opengauss.datachecker.common.thread;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.LogUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * ThreadPoolFactory
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/17
 * @since ：11
 */
public class ThreadPoolFactory {
    private static final Logger log = LogUtils.getLogger(ThreadPoolFactory.class);

    private static final double TARGET_UTILIZATION = 0.5d;
    private static final double IO_WAIT_TIME = 1.0d;
    private static final double CPU_TIME = 1.0d;
    private static final double POOL_QUEUE_EXPANSION_RATIO = 1.2d;
    private static final double CORE_POOL_SIZE_RATIO = 2.0d;
    private static final int DEFAULT_QUEUE_SIZE = Integer.MAX_VALUE;

    /**
     * Initialize the extract service thread pool
     *
     * @param threadName threadName
     * @param queueSize  queueSize
     * @return ExecutorService
     */
    public static ThreadPoolExecutor newThreadPool(String threadName, int queueSize) {
        return createThreadPool(threadName, queueSize);
    }

    /**
     * Initialize the extract service thread pool
     *
     * @param threadName   threadName
     * @param corePoolSize corePoolSize
     * @param queueSize    queueSize
     * @return ExecutorService
     */
    public static ThreadPoolExecutor newThreadPool(String threadName, int corePoolSize, int queueSize) {
        if (corePoolSize <= 0) {
            return createThreadPool(threadName, queueSize);
        }
        int maxCorePoolSize = corePoolSize * 2;
        return createThreadPool(threadName, corePoolSize, maxCorePoolSize, queueSize);
    }

    /**
     * Initialize the service thread pool
     *
     * @param threadName      threadName
     * @param corePoolSize    corePoolSize
     * @param maximumPoolSize maximumPoolSize
     * @param queueSize       queueSize
     * @return ExecutorService
     */
    public static ThreadPoolExecutor newThreadPool(String threadName, int corePoolSize, int maximumPoolSize,
        int queueSize) {
        if (corePoolSize <= 0) {
            return createThreadPool(threadName, queueSize);
        }
        return createThreadPool(threadName, corePoolSize, maximumPoolSize, queueSize);
    }

    private static ThreadPoolExecutor createThreadPool(String threadName, int size) {
        int queueSize = calculateCheckQueueCapacity(size);
        int threadNum = calculateOptimalThreadCount(CPU_TIME, IO_WAIT_TIME, TARGET_UTILIZATION);
        int corePoolSize = calculateCorePoolSize(threadNum);
        return createThreadPool(threadName, corePoolSize, threadNum, queueSize);
    }

    private static ThreadPoolExecutor createThreadPool(String threadName, int corePoolSize, int threadNum,
        int queueSize) {
        if (queueSize <= 0) {
            queueSize = DEFAULT_QUEUE_SIZE;
        }
        BlockingQueue<Runnable> blockingQueue = new LinkedBlockingQueue<>(queueSize);
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(corePoolSize, threadNum, 60L, TimeUnit.SECONDS,
            blockingQueue, r -> {
            Thread t = new Thread(r);
            t.setUncaughtExceptionHandler((thread, throwable) -> {
                if (throwable instanceof OutOfMemoryError) {
                    LogUtils.error(log, "OOM in thread: {}", thread.getName());
                    throwable.printStackTrace();
                    Runtime.getRuntime().halt(1);
                }
            });
            return t;
        }, new DiscardOldestPolicy(log, threadName));
        threadPoolExecutor.allowCoreThreadTimeOut(true);
        LogUtils.debug(log, "Thread name is {},cpu={} corePoolSize is : {}, size is {}, queueSize is {}", threadName,
            getNumberOfCpu(), corePoolSize, threadNum, queueSize);
        return threadPoolExecutor;
    }

    private static int calculateCheckQueueCapacity(int size) {
        return (int) Math.ceil(size * POOL_QUEUE_EXPANSION_RATIO);
    }

    private static int calculateCorePoolSize(int threadNum) {
        final int core = (int) Math.ceil(threadNum / CORE_POOL_SIZE_RATIO);
        return Math.min(core, 50);
    }

    private static int calculateOptimalThreadCount(double computeTime, double waitTime, double targetUtilization) {
        int numberOfCpu = getNumberOfCpu();
        final int threadNum =
            (int) Math.ceil(numberOfCpu * targetUtilization * (Math.round(waitTime / computeTime) + 1));
        return Math.min(threadNum, 100);
    }

    private static int getNumberOfCpu() {
        return Runtime.getRuntime()
                      .availableProcessors();
    }
}
