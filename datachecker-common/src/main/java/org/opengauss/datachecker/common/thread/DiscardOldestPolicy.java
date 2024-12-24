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
import org.opengauss.datachecker.common.entry.enums.ErrorCode;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DiscardOldestPolicy
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/17
 * @since ：11
 */
public class DiscardOldestPolicy extends ThreadPoolExecutor.DiscardOldestPolicy {
    private AtomicLong discard = new AtomicLong(0);
    private Logger logger;
    private String threadName = "";

    public DiscardOldestPolicy(Logger logger) {
        this.logger = logger;
    }

    public DiscardOldestPolicy(Logger logger, String threadName) {
        this.logger = logger;
        this.threadName = threadName;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
        super.rejectedExecution(r, e);
        final long rejectedSum = discard.incrementAndGet();
        if (rejectedSum == 1 || rejectedSum % 100 == 0) {
            logger.error(
                "{}DiscardOldest worker, had discard {}, taskCount {}, completedTaskCount {}, largestPoolSize {},"
                    + "getPoolSize {}, getActiveCount {}, getThreadName {}", ErrorCode.UNKNOWN, rejectedSum,
                e.getTaskCount(), e.getCompletedTaskCount(), e.getLargestPoolSize(), e.getPoolSize(),
                e.getActiveCount(), threadName);
        }
    }
}