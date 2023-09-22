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

package org.opengauss.datachecker.common.service;

import lombok.SneakyThrows;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.constant.DynamicTpConstant;
import org.opengauss.datachecker.common.entry.memory.CpuInfo;
import org.opengauss.datachecker.common.entry.memory.JvmInfo;
import org.opengauss.datachecker.common.util.LogUtils;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * DynamicThreadPoolMonitor
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/24
 * @since ：11
 */
public class DynamicThreadPoolMonitor implements Runnable {
    private static final Logger log = LogUtils.getDebugLogger();
    private int initCorePoolSize;
    private Map<String, ThreadPoolExecutor> executors;
    private volatile boolean isChecked = true;

    /**
     * DynamicThreadPoolMonitor
     *
     * @param executorServiceCache executorServiceCache
     * @param initCorePoolSize     initCorePoolSize
     */
    public DynamicThreadPoolMonitor(Map<String, ThreadPoolExecutor> executorServiceCache, int initCorePoolSize) {
        this.executors = executorServiceCache;
        this.initCorePoolSize = initCorePoolSize;
    }

    @SneakyThrows
    @Override
    public void run() {
        while (isChecked) {
            CpuInfo cpuInfo = MemoryManager.getCpuInfo();
            JvmInfo jvmInfo = MemoryManager.getJvmInfo();
            executors.forEach((name, tpExecutor) -> {
                if (Double.compare(cpuInfo.getFree(), DynamicTpConstant.MIN_CPU_FREE) < 0) {
                    decrementCorePoolSize(tpExecutor);
                    return;
                }
                if (Double.compare(jvmInfo.getFree(), DynamicTpConstant.MIN_MEMORY_FREE) < 0) {
                    decrementCorePoolSize(tpExecutor);
                    return;
                }
                if (hasWaitTask(tpExecutor)) {
                    incrementCorePoolSize(tpExecutor, cpuInfo);
                } else {
                    decrementCorePoolSize(tpExecutor);
                }
                if (hasNoWaitTask(tpExecutor)) {
                    setMinCorePoolSize(tpExecutor);
                }
                logInfo(name, tpExecutor);
            });
            Thread.sleep(DynamicTpConstant.MONITORING_PERIOD_MILLIS);
        }
    }

    /**
     * close dynamic thread pool monitor
     */
    public void closeMonitor() {
        isChecked = false;
    }

    private void logInfo(String name, ThreadPoolExecutor tpExecutor) {
        if (tpExecutor.getActiveCount() > 0) {
            log.info(
                "DynamicThreadPoolMonitor {} coreSize={}, maxSize={}, taskCount={}, completedCount={}, activeCount={}",
                name, tpExecutor.getCorePoolSize(), tpExecutor.getMaximumPoolSize(), tpExecutor.getTaskCount(),
                tpExecutor.getCompletedTaskCount(), tpExecutor.getActiveCount());
        }
    }

    private void decrementCorePoolSize(ThreadPoolExecutor tpExecutor) {
        int corePoolSize = tpExecutor.getCorePoolSize();
        int activeCount = tpExecutor.getActiveCount();
        int resetCoreSize = Math.max(activeCount, initCorePoolSize);
        if (corePoolSize > initCorePoolSize && resetCoreSize != corePoolSize) {
            tpExecutor.setCorePoolSize(resetCoreSize);
        }
    }

    private void setMinCorePoolSize(ThreadPoolExecutor tpExecutor) {
        if (tpExecutor.getCorePoolSize() != initCorePoolSize) {
            tpExecutor.setCorePoolSize(initCorePoolSize);
        }
    }

    private void incrementCorePoolSize(ThreadPoolExecutor tpExecutor, CpuInfo cpuInfo) {
        int corePoolSize = tpExecutor.getCorePoolSize();
        int maximumPoolSize = tpExecutor.getMaximumPoolSize();
        if (corePoolSize < maximumPoolSize && Double.compare(cpuInfo.getFree(), DynamicTpConstant.MIN_CPU_FREE) > 0) {
            tpExecutor.setCorePoolSize(corePoolSize + 1);
        }
    }

    private boolean hasNoWaitTask(ThreadPoolExecutor tpExecutor) {
        return tpExecutor.getTaskCount() == (tpExecutor.getCompletedTaskCount() + tpExecutor.getActiveCount());
    }

    private boolean hasWaitTask(ThreadPoolExecutor tpExecutor) {
        return (tpExecutor.getTaskCount() - tpExecutor.getCompletedTaskCount() - tpExecutor.getActiveCount()) > 1;
    }
}