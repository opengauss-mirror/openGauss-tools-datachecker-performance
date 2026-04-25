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
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import feign.FeignException;

import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * SliceStatusFeedbackService
 * Slice status feedback service, responsible for reporting slice processing status to the checking service
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
@Slf4j
public class SliceStatusFeedbackService {
    private static final int MAX_RETRIES = 3;
    private static final long BASE_RETRY_DELAY_MS = 500L;
    private static final long MAX_RETRY_DELAY_MS = 2000L;
    private static final long QUEUE_POLL_TIMEOUT_MS = 1000L;
    private static final String FEEDBACK_THREAD_NAME = "status-feedback-service";

    private final BlockingQueue<SliceExtend> feedbackQueue = new LinkedBlockingQueue<>();
    private final ExecutorService feedbackSender;
    private final CheckingFeignClient checkingClient;
    private volatile boolean isCompleted = false;

    /**
     * Constructor
     *
     * @param checkingClient Checking service client
     */
    public SliceStatusFeedbackService(CheckingFeignClient checkingClient) {
        this.checkingClient = checkingClient;
        this.feedbackSender = ThreadUtil.newSingleThreadExecutor();
    }

    /**
     * Add slice status feedback task to queue
     *
     * @param sliceExtend Slice extend information
     */
    public void addFeedbackStatus(SliceExtend sliceExtend) {
        if (!feedbackQueue.offer(sliceExtend)) {
            log.warn("Failed to add feedback to queue, queue might be full: {}", sliceExtend.getName());
            try {
                sliceExtend.setEndpoint(ConfigCache.getEndPoint());
                checkingClient.refreshRegisterSlice(sliceExtend);
            } catch (FeignException e) {
                log.error("Failed to send feedback directly: {}", e.getMessage());
            }
        }
    }

    /**
     * Stop slice status feedback thread
     */
    public void stop() {
        this.isCompleted = true;
        this.feedbackSender.shutdownNow();
        log.info("Slice status feedback service stopped");
    }

    /**
     * Start slice status feedback thread
     */
    public void feedback() {
        feedbackSender.submit(() -> {
            Thread.currentThread().setName(FEEDBACK_THREAD_NAME);
            log.info("Slice status feedback service started");

            while (!isCompleted) {
                try {
                    processNextFeedbackTask();
                } catch (Exception ex) {
                    log.error("Error in feedback thread: {}", ex.getMessage(), ex);
                    // Sleep briefly and continue to avoid thread exit due to exceptions
                    ThreadUtil.sleepOneSecond();
                }
            }

            log.info("Slice status feedback service completed and exited");
        });
    }

    /**
     * Process next feedback task
     */
    private void processNextFeedbackTask() {
        SliceExtend sliceExt = null;
        try {
            // 使用带超时的 poll 方法
            sliceExt = feedbackQueue.poll(QUEUE_POLL_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (isCompleted) {
                return;
            }
        }

        if (Objects.isNull(sliceExt)) {
            if (isCompleted) {
                log.debug("Feedback service completed, exiting");
            }
        } else {
                sliceExt.setEndpoint(ConfigCache.getEndPoint());
                log.info("Feedback slice status of table [{}]", sliceExt);
                refreshRegisterSliceWithRetry(sliceExt);
        }
    }

    /**
     * Refresh register slice with retry mechanism
     *
     * @param sliceExt Slice extend information
     */
    private void refreshRegisterSliceWithRetry(SliceExtend sliceExt) {
        int retryCount = 0;
        boolean isSuccess = false;

        while (!isSuccess && retryCount < MAX_RETRIES) {
            try {
                checkingClient.refreshRegisterSlice(sliceExt);
                isSuccess = true;
                log.debug("Successfully refreshed register slice: {}", sliceExt.getName());
            } catch (FeignException e) {
                retryCount++;
                if (retryCount < MAX_RETRIES) {
                    long delayMs = calculateBackoffDelay(retryCount);
                    log.info("Refresh register slice failed, retrying {}/{} after {}ms: {}",
                            retryCount, MAX_RETRIES, delayMs, e.getMessage());
                    ThreadUtil.sleep(delayMs);
                } else {
                    log.error("Refresh register slice failed after {} retries: {}", MAX_RETRIES, e.getMessage());
                    throw new ExtractException(e.getMessage());
                }
            }
        }
    }

    /**
     * Calculate backoff delay time
     *
     * @param retryCount Retry count
     * @return Delay time (milliseconds)
     */
    private long calculateBackoffDelay(int retryCount) {
        // Exponential backoff strategy: baseDelay * (2^retryCount), maximum not exceeding maxDelay
        long delayMs = BASE_RETRY_DELAY_MS * (1L << retryCount);
        return Math.min(delayMs, MAX_RETRY_DELAY_MS);
    }
}
