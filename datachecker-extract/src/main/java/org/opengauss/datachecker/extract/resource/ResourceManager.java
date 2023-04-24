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

package org.opengauss.datachecker.extract.resource;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.memory.JvmInfo;
import org.opengauss.datachecker.common.service.MemoryManager;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.extract.config.DruidDataSourceConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ResourceManager
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/25
 * @since ：11
 */
@Slf4j
@Service
@ConditionalOnBean(DruidDataSourceConfig.class)
public class ResourceManager {
    private static final int MAX_AVAILABLE_TIMES = 30;

    private volatile AtomicInteger connectionCount = new AtomicInteger(0);
    private volatile AtomicInteger tryAvailableTimes = new AtomicInteger(0);

    @Value("${spring.extract.query-dop}")
    private int queryDop;
    @Resource
    private DruidDataSourceConfig dataSourceConfig;
    @Resource
    private ShutdownService shutdownService;
    private ReentrantLock lock = new ReentrantLock();

    /**
     * initMaxConnectionCount
     */
    @PostConstruct
    public void initMaxConnectionCount() {
        DataSource dataSource = dataSourceConfig.druidDataSource();
        if (dataSource instanceof DruidDataSource) {
            final DruidDataSource druidDataSource = (DruidDataSource) dataSource;
            connectionCount.set(druidDataSource.getMaxActive());
            final JvmInfo memory = MemoryManager.getJvmInfo();
            log.info("max active connection {} ,max memory {}", connectionCount.get(), memory.getMax());
        }
    }

    /**
     * queryDop
     *
     * @return queryDop
     */
    public int getParallelQueryDop() {
        return queryDop;
    }

    /**
     * Check whether there are available database link resources and memory resources with a specified memory size.
     * If the resource cannot be obtained for 30 consecutive times, actively perform a GC once
     *
     * @param freeSize Free Memory Size Bytes
     * @return Currently implemented
     */
    public boolean canExecQuery(long freeSize) {
        lock.lock();
        try {
            final JvmInfo memory = MemoryManager.getJvmInfo();
            if (connectionCount.get() > 2 && memory.isAvailable(freeSize)) {
                connectionCount.decrementAndGet();
                tryAvailableTimes.set(0);
                return true;
            }
        } finally {
            if (tryAvailableTimes.get() >= MAX_AVAILABLE_TIMES) {
                Runtime.getRuntime().gc();
            }
            lock.unlock();
        }
        tryAvailableTimes.incrementAndGet();
        return false;
    }

    /**
     * check has free connection
     *
     * @return canExecQuery
     */
    public boolean canExecQuery() {
        lock.lock();
        try {
            if (connectionCount.get() > 1) {
                connectionCount.decrementAndGet();
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    /**
     * check service is shutdown
     *
     * @return isShutdown
     */
    public boolean isShutdown() {
        return shutdownService.isShutdown();
    }

    /**
     * release connection count
     */
    public void release() {
        lock.lock();
        try {
            connectionCount.incrementAndGet();
        } finally {
            lock.unlock();
        }
    }
}
