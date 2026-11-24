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

package org.opengauss.datachecker.common.service;

import jakarta.annotation.Resource;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.scheduling.concurrent.ExecutorConfigurationSupport;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/14
 * @since ：11
 */
@Service
public class ShutdownService {
    private static final Logger log = LogUtils.getLogger(ShutdownService.class);

    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicInteger monitor = new AtomicInteger(0);
    private List<ExecutorService> executorServiceList = new LinkedList<>();
    private List<ThreadPoolTaskExecutor> threadExecutorList = new LinkedList<>();
    @Resource
    private DynamicThreadPoolManager dynamicThreadPoolManager;
    @Resource
    private ThreadPoolTaskExecutor sliceSendExecutor;
    @Resource
    private ProcessLogService processLogService;

    public void shutdown(String message) {
        // 优先写入 stop 进程日志，确保 JVM 退出前结束标识落盘，避免与 System.exit 抢跑导致丢失
        processLogService.saveStopProcessLog();
        LogUtils.info(log, "The check server will be shutdown , {} . check server exited .", message);
        ThreadUtil.sleep(ConfigCache.getIntValue(ConfigConstants.TIMEOUT_PER_SHUTDOWN_PHASE));
        LogUtils.info(log, "The check server wait 5s and will be shutdown , {} . check server exited .", message);
        isShutdown.set(true);
        dynamicThreadPoolManager.closeDynamicThreadPoolMonitor();
        while (monitor.get() > 0) {
            ThreadUtil.sleepHalfSecond();
        }
        threadExecutorList.forEach(ExecutorConfigurationSupport::shutdown);
        executorServiceList.forEach(ExecutorService::shutdownNow);
        sliceSendExecutor.shutdown();
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public synchronized int addMonitor() {
        return monitor.incrementAndGet();
    }

    public synchronized int releaseMonitor() {
        return monitor.decrementAndGet();
    }

    public void addExecutorService(ExecutorService scheduledExecutor) {
        executorServiceList.add(scheduledExecutor);
    }

    public void addThreadPoolTaskExecutor(ThreadPoolTaskExecutor executor) {
        threadExecutorList.add(executor);
    }
}
