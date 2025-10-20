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

package org.opengauss.datachecker.check.service;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.common.Health;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadPoolShutdownUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;

import java.util.concurrent.ExecutorService;

/**
 * Data Extraction Service Endpoint Management
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/26
 * @since ：11
 */
@Service
public class EndpointManagerService {
    private static final Logger log = LogUtils.getLogger(EndpointManagerService.class);
    private static final long INITIAL_CHECK_INTERVAL = 1000L;
    private static final long FINAL_CHECK_INTERVAL = 30000L;
    private static final long WARMUP_DURATION = 120000L;
    private static final long TRANSITION_DURATION = 180000L;
    private static final ExecutorService executorService = ThreadUtil.newSingleThreadExecutor();
    @Value("${data.check.retry-interval-times}")
    protected int retryIntervalTimes;
    @Autowired
    private FeignClientService feignClientService;
    @Autowired
    private DataCheckProperties dataCheckProperties;
    @Autowired
    private EndpointStatusManager endpointStatusManager;
    @Resource
    private ShutdownService shutdownService;

    /**
     * View the health status of all endpoints
     *
     * @return health status
     */
    public boolean isEndpointHealth() {
        return endpointStatusManager.isEndpointHealth();
    }

    public void heartBeat() {
        if (ConfigCache.getBooleanValue(ConfigConstants.ENABLE_HEART_BEAT_HEATH)) {
            executorService.submit(this::endpointHealthCheck);
        }
    }

    /**
     * Endpoint health check
     */
    public void endpointHealthCheck() {
        Thread.currentThread().setName("heart-beat-heath");
        shutdownService.addMonitor();
        try {
            long startTime = System.currentTimeMillis();
            long lastCheckTime = startTime;
            while (!(shutdownService.isShutdown() || executorService.isShutdown())) {
                long currentTime = System.currentTimeMillis();
                long elapsed = currentTime - startTime;
                long checkInterval = calculateCheckInterval(elapsed);
                if (currentTime - lastCheckTime >= checkInterval) {
                    checkEndpoint(dataCheckProperties.getSourceUri(), Endpoint.SOURCE, "source endpoint service check");
                    checkEndpoint(dataCheckProperties.getSinkUri(), Endpoint.SINK, "sink endpoint service check");
                    lastCheckTime = currentTime;
                }
                // 短暂休眠避免CPU过度占用
                ThreadUtil.sleep(100);
            }
        } finally {
            shutdownService.releaseMonitor();
        }
    }

    /**
     * Dynamically calculates the heartbeat check interval
     * <p>
     * Rules:
     * 1. First 2 minutes (warm-up period): Use initial interval (1 second)
     * 2. 2-5 minutes (transition period): Linearly increase interval from 1s to 10s
     * 3. After 5 minutes: Use final interval (10 seconds)
     *
     * @param elapsedMillis elapsedMillis
     * @return check interval
     */
    private long calculateCheckInterval(long elapsedMillis) {
        if (elapsedMillis < WARMUP_DURATION) {
            return INITIAL_CHECK_INTERVAL;
        } else if (elapsedMillis < WARMUP_DURATION + TRANSITION_DURATION) {
            double progress = (double) (elapsedMillis - WARMUP_DURATION) / (double) TRANSITION_DURATION;
            return INITIAL_CHECK_INTERVAL + (long) (progress * (double) (FINAL_CHECK_INTERVAL
                - INITIAL_CHECK_INTERVAL));
        } else {
            return FINAL_CHECK_INTERVAL;
        }
    }

    public boolean checkEndpointHealth(Endpoint endpoint) {
        return endpointStatusManager.getHealthStatus(endpoint);
    }

    private void checkEndpoint(String requestUri, Endpoint endpoint, String message) {
        // service network check ping
        try {
            // service check: service database check
            Result<Health> healthStatus = feignClientService.health(endpoint);
            if (healthStatus.isSuccess()) {
                if (healthStatus.getData().isHealth()) {
                    endpointStatusManager.resetStatus(endpoint, Boolean.TRUE);
                } else {
                    endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
                }
                LogUtils.info(log, "{} ：{} status {}", message, requestUri, healthStatus.getData());
            } else {
                endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
                LogUtils.warn(log, "{} : {} status [{}]", message, requestUri, healthStatus.getData());
            }
        } catch (Exception ce) {
            LogUtils.warn(log, "{} : {} service unreachable", message, ce.getMessage());
            endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
        }
    }

    @PreDestroy
    public void stopHeartBeat() {
        endpointStatusManager.resetStatus(Endpoint.SOURCE, false);
        endpointStatusManager.resetStatus(Endpoint.SINK, false);
        ThreadPoolShutdownUtil.shutdown(executorService, "heart-beat-heath");
    }
}
