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

package org.opengauss.datachecker.common.exception;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.util.LogUtils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Custom Check Thread Factory
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/18
 * @since ：11
 */
public class CheckThreadFactory implements ThreadFactory {
    private static final AtomicInteger POOL_NUMBER = new AtomicInteger(1);

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    /**
     * CheckThreadFactory
     */
    public CheckThreadFactory() {
        group = Thread.currentThread().getThreadGroup();
        namePrefix = "pool-" + POOL_NUMBER.getAndIncrement() + "-thread-";
    }

    /**
     * newThread
     *
     * @param runnable runnable
     * @return thread
     */
    @SuppressWarnings("NullableProblems")
    @Override
    public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement(), 0);
        if (thread.isDaemon()) {
            thread.setDaemon(false);
        }
        // UncaughtExceptionHandler
        thread.setUncaughtExceptionHandler(new CheckUncaughtExceptionHandler());
        if (thread.getPriority() != Thread.NORM_PRIORITY) {
            thread.setPriority(Thread.NORM_PRIORITY);
        }
        return thread;
    }
}

/**
 * Custom Check Thread Exception Handler
 */
class CheckUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private static final Logger log = LogUtils.getLogger();

    @Override
    public void uncaughtException(Thread thread, Throwable throwable) {
        log.error("{}{} exception: ", ErrorCode.UNKNOWN, thread.getName(), throwable);
    }
}
