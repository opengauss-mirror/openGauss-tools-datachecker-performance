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
    private static final long MAX_CHECK_INTERVAL = 10000L;
    private static final double BACKOFF_MULTIPLIER = 2.0;
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
            HealthCheckState state = new HealthCheckState();
            while (!isShutdown()) {
                if (state.shouldPerformCheck()) {
                    performHealthCheck(state);
                }
                ThreadUtil.sleepOneSecond();
            }
        } finally {
            shutdownService.releaseMonitor();
        }
    }

    private boolean isShutdown() {
        return shutdownService.isShutdown() || executorService.isShutdown();
    }

    private void performHealthCheck(HealthCheckState state) {
        boolean isSourceHealthy = checkEndpoint(dataCheckProperties.getSourceUri(), Endpoint.SOURCE,
                "source endpoint service check");
        boolean isSinkHealthy = checkEndpoint(dataCheckProperties.getSinkUri(), Endpoint.SINK,
                "sink endpoint service check");
        state.update(isSourceHealthy && isSinkHealthy);
    }

    public boolean checkEndpointHealth(Endpoint endpoint) {
        return endpointStatusManager.getHealthStatus(endpoint);
    }

    private boolean checkEndpoint(String requestUri, Endpoint endpoint, String message) {
        try {
            Result<Health> healthStatus = feignClientService.health(endpoint);
            if (healthStatus.isSuccess()) {
                if (healthStatus.getData().isHealth()) {
                    endpointStatusManager.resetStatus(endpoint, Boolean.TRUE);
                    LogUtils.info(log, "{} : {} status {}", message, requestUri, healthStatus.getData());
                    return true;
                } else {
                    endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
                    LogUtils.warn(log, "{} : {} status [{}]", message, requestUri, healthStatus.getData());
                    return false;
                }
            } else {
                endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
                LogUtils.warn(log, "{} : {} status [{}]", message, requestUri, healthStatus.getData());
                return false;
            }
        } catch (Exception ce) {
            LogUtils.warn(log, "{} : {} service unreachable", message, ce.getMessage());
            endpointStatusManager.resetStatus(endpoint, Boolean.FALSE);
            return false;
        }
    }

    @PreDestroy
    public void stopHeartBeat() {
        endpointStatusManager.resetStatus(Endpoint.SOURCE, false);
        endpointStatusManager.resetStatus(Endpoint.SINK, false);
        ThreadPoolShutdownUtil.shutdown(executorService, "heart-beat-heath");
    }

    private static class HealthCheckState {
        private long lastCheckTime = System.currentTimeMillis();
        private long currentInterval = INITIAL_CHECK_INTERVAL;
        private int consecutiveFailures = 0;

        boolean shouldPerformCheck() {
            return System.currentTimeMillis() - lastCheckTime >= currentInterval;
        }

        void update(boolean isAllHealthy) {
            lastCheckTime = System.currentTimeMillis();
            if (isAllHealthy) {
                resetBackoff();
            } else {
                incrementBackoff();
            }
        }

        private void resetBackoff() {
            consecutiveFailures = 0;
            currentInterval = INITIAL_CHECK_INTERVAL;
        }

        private void incrementBackoff() {
            consecutiveFailures++;
            currentInterval = calculateExponentialBackoffInterval(consecutiveFailures);
            if (currentInterval >= MAX_CHECK_INTERVAL) {
                consecutiveFailures = 0;
            }
        }

        private long calculateExponentialBackoffInterval(int consecutiveFailures) {
            long interval = (long) (INITIAL_CHECK_INTERVAL * Math.pow(BACKOFF_MULTIPLIER, consecutiveFailures));
            return Math.min(interval, MAX_CHECK_INTERVAL);
        }
    }
}
