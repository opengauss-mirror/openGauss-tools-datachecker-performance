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

import org.opengauss.datachecker.common.constant.DynamicTpConstant;
import org.opengauss.datachecker.common.exception.ExtractBootstrapException;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * DynamicThreadPoolManager
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/23
 * @since ：11
 */
@Component
public class DynamicThreadPoolManager {
    private static final Map<String, ThreadPoolExecutor> EXECUTOR_SERVICE_CACHE = new ConcurrentHashMap<>();

    private volatile DynamicThreadPoolMonitor monitor;
    @Resource
    private DynamicThreadPool dynamicThreadPool;
    @Value("${spring.check.core-pool-size}")
    private int corePoolSize;
    @Value("${spring.check.maximum-pool-size}")
    private int maximumPoolSize;

    /**
     * buildDynamicThreadPoolExecutor
     */
    @PostConstruct
    public void buildDynamicThreadPoolExecutor() {
        if (corePoolSize > maximumPoolSize) {
            throw new ExtractBootstrapException("core-pool-size can not bigger than maximum-pool-size");
        }
        corePoolSize = corePoolSize > 0 ? corePoolSize : DynamicTpConstant.DEFAULT_CORE_POOL_SIZE;
        maximumPoolSize = maximumPoolSize > 0 ? maximumPoolSize : DynamicTpConstant.DEFAULT_MAXIMUM_POOL_SIZE;
        dynamicThreadPool.buildDynamicThreadPoolExecutor(EXECUTOR_SERVICE_CACHE, corePoolSize, maximumPoolSize);
    }

    /**
     * start dynamicThreadPoolMonitor
     */
    public void dynamicThreadPoolMonitor() {
        monitor = new DynamicThreadPoolMonitor(EXECUTOR_SERVICE_CACHE, corePoolSize);
        new Thread(monitor).start();
    }

    /**
     * close dynamic thread pool monitor
     */
    public void closeDynamicThreadPoolMonitor() {
        if (monitor != null) {
            monitor.closeMonitor();
        }
        EXECUTOR_SERVICE_CACHE.forEach((name, executor) -> {
            executor.shutdownNow();
        });
    }

    /**
     * get Dynamic Thread Pool Executor
     *
     * @param name thread pool name
     * @return ThreadPoolExecutor
     */
    public ThreadPoolExecutor getExecutor(String name) {
        return EXECUTOR_SERVICE_CACHE.get(name);
    }

    /**
     * get Extend Free Thread Pool Executor
     *
     * @param topicSize         topicSize
     * @param extendMaxPoolSize extendMaxPoolSize
     * @return ExecutorService
     */
    public ExecutorService getFreeExecutor(int topicSize, int extendMaxPoolSize) {
        List<String> freeDtpList = getFreeDtpList();
        if (freeDtpList.size() > 0) {
            return EXECUTOR_SERVICE_CACHE.get(freeDtpList.get(0));
        }
        long count = EXECUTOR_SERVICE_CACHE.keySet()
                                           .stream()
                                           .filter(dtpName -> dtpName.startsWith(DynamicTpConstant.EXTEND_EXECUTOR))
                                           .count();
        if (count < topicSize) {
            return buildExtendDtpExecutor(DynamicTpConstant.EXTEND_EXECUTOR + (count + 1), extendMaxPoolSize);
        } else {
            ThreadUtil.sleepOneSecond();
            return getFreeExecutor(topicSize, extendMaxPoolSize);
        }
    }

    private List<String> getFreeDtpList() {
        List<String> freeDtpList = new LinkedList<>();
        EXECUTOR_SERVICE_CACHE.forEach((dtpName, dtpPool) -> {
            if (dtpName.startsWith(DynamicTpConstant.EXTEND_EXECUTOR) && dtpPool.getQueue()
                                                                                .isEmpty()
                && dtpPool.getActiveCount() == 0) {
                freeDtpList.add(dtpName);
            }
        });
        return freeDtpList;
    }

    private ThreadPoolExecutor buildExtendDtpExecutor(String extendDtpName, int extendMaxPoolSize) {
        dynamicThreadPool.buildExtendDtpExecutor(EXECUTOR_SERVICE_CACHE, extendDtpName, corePoolSize, extendMaxPoolSize);
        return EXECUTOR_SERVICE_CACHE.get(extendDtpName);
    }

    public boolean allExecutorFree() {
        List<String> freeDtpList = new LinkedList<>();
        EXECUTOR_SERVICE_CACHE.forEach((dtpName, dtpPool) -> {
            if (dtpPool.getQueue()
                       .isEmpty() && dtpPool.getActiveCount() == 0) {
                freeDtpList.add(dtpName);
            }
        });
        return EXECUTOR_SERVICE_CACHE.size() == freeDtpList.size();
    }
}
