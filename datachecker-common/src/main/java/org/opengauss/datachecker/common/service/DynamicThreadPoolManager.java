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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    @Async
    public void dynamicThreadPoolMonitor() {
        monitor = new DynamicThreadPoolMonitor(EXECUTOR_SERVICE_CACHE, corePoolSize);
        monitor.run();
    }

    /**
     * close dynamic thread pool monitor
     */
    public void closeDynamicThreadPoolMonitor() {
        monitor.closeMonitor();
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
}
