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

package org.opengauss.datachecker.extract.slice;

import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.springframework.stereotype.Component;

import cn.hutool.core.thread.ThreadUtil;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

/**
 * SliceRegister
 * Slice registration service, responsible for registering slices to the checking service
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/21
 * @since ：11
 */
@Slf4j
@Component
public class SliceRegister {
    private static final int MAX_RETRIES = 3;
    private static final int BATCH_SIZE = 5;
    private static final long BASE_RETRY_DELAY_MS = 500L;
    private static final long MAX_RETRY_DELAY_MS = 2000L;
    private static final long BATCH_DELAY_MS = 200L;
    private static final long TEN_SLICE_DELAY_MS = 500L;

    @Resource
    private CheckingFeignClient checkingClient;
    private final Semaphore semaphore = new Semaphore(3); // 增加信号量许可数

    /**
     * Register slices to check service in batches
     *
     * @param sliceList List of slices to register
     */
    public void batchRegister(List<SliceVo> sliceList) {
        List<SliceVo> batchList = new ArrayList<>();
        int count = 0;

        for (SliceVo sliceVo : sliceList) {
            sliceVo.setEndpoint(ConfigCache.getEndPoint());
            batchList.add(sliceVo);

            if (batchList.size() >= BATCH_SIZE) {
                log.debug("Processing batch of {} slices", batchList.size());
                batchRegisterWithRetry(batchList);
                batchList.clear();
                ThreadUtil.safeSleep(BATCH_DELAY_MS);
            }

            if (++count % 10 == 0) {
                log.debug("Adding delay after processing 10 slices");
                ThreadUtil.safeSleep(TEN_SLICE_DELAY_MS);
            }
        }

        if (!batchList.isEmpty()) {
            log.debug("Processing final batch of {} slices", batchList.size());
            batchRegisterWithRetry(batchList);
        }
    }

    /**
     * Batch register slices with retry mechanism
     *
     * @param sliceList List of slices to register
     */
    private void batchRegisterWithRetry(List<SliceVo> sliceList) {
        int retryCount = 0;
        boolean isSuccess = false;

        while (!isSuccess && retryCount < MAX_RETRIES) {
            try {
                semaphore.acquire();
                try {
                    checkingClient.batchRegisterSlice(sliceList);
                    isSuccess = true;
                    log.debug("Successfully registered batch of {} slices", sliceList.size());
                } finally {
                    semaphore.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted while acquiring semaphore: {}", e.getMessage());
                throw new ExtractException(e.getMessage());
            } catch (Exception e) {
                retryCount++;
                if (retryCount < MAX_RETRIES) {
                    long delayMs = calculateBackoffDelay(retryCount);
                    log.info("Batch register slice failed, retrying {}/{} after {}ms: {}",
                            retryCount, MAX_RETRIES, delayMs, e.getMessage());
                    ThreadUtil.safeSleep(delayMs);
                } else {
                    log.error("Batch register slice failed after {} retries: {}", MAX_RETRIES, e.getMessage());
                    throw new ExtractException(e.getMessage());
                }
            }
        }
    }

    /**
     * Start table checkpoint monitor
     */
    public void startCheckPointMonitor() {
        executeWithRetry(() -> checkingClient.startCheckPointMonitor(), "startCheckPointMonitor");
    }

    /**
     * Stop table checkpoint monitor
     *
     * @param endpoint Endpoint
     */
    public void stopCheckPointMonitor(Endpoint endpoint) {
        executeWithRetry(() -> checkingClient.stopCheckPointMonitor(endpoint), "stopCheckPointMonitor");
    }

    /**
     * Execute operation with retry mechanism
     *
     * @param operation     Operation to execute
     * @param operationName Operation name for logging
     */
    private void executeWithRetry(Runnable operation, String operationName) {
        int retryCount = 0;
        boolean isSuccess = false;

        while (!isSuccess && retryCount < MAX_RETRIES) {
            try {
                semaphore.acquire();
                try {
                    operation.run();
                    isSuccess = true;
                    log.debug("Successfully executed operation: {}", operationName);
                } finally {
                    semaphore.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Thread interrupted while acquiring semaphore: {}", e.getMessage());
            } catch (Exception e) {
                retryCount++;
                if (retryCount < MAX_RETRIES) {
                    long delayMs = calculateBackoffDelay(retryCount);
                    log.info("{} failed, retrying {}/{} after {}ms: {}",
                            operationName, retryCount, MAX_RETRIES, delayMs, e.getMessage());
                    ThreadUtil.safeSleep(delayMs);
                } else {
                    log.error("{} failed after {} retries: {}", operationName, MAX_RETRIES, e.getMessage());
                    throw new ExtractException(e.getMessage());
                }
            }
        }
    }

    /**
     * Calculate backoff delay time
     * Exponential backoff strategy: baseDelay * (2^retryCount), maximum not exceeding maxDelay
     *
     * @param retryCount Retry count
     * @return Delay time (milliseconds)
     */
    private long calculateBackoffDelay(int retryCount) {
        long delayMs = BASE_RETRY_DELAY_MS * (1 << retryCount);
        return Math.min(delayMs, MAX_RETRY_DELAY_MS);
    }
}