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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SliceStatusFeedbackService
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public class SliceStatusFeedbackService {
    private static final Logger log = LogUtils.getLogger(SliceStatusFeedbackService.class);
    private static final Lock lock = new ReentrantLock();
    public static final String FEEDBACK_THREAD_NAME = "status-feedback-service";
    private final BlockingQueue<SliceExtend> feedbackQueue = new LinkedBlockingQueue<>();
    private final ExecutorService feedbackSender;
    private final CheckingFeignClient checkingClient;
    private boolean isCompleted = false;

    public SliceStatusFeedbackService(CheckingFeignClient checkingClient) {
        this.checkingClient = checkingClient;
        this.feedbackSender = ThreadUtil.newSingleThreadExecutor();
    }

    /**
     * add slice status task for queue
     *
     * @param sliceExtend sliceExtend
     */
    public void addFeedbackStatus(SliceExtend sliceExtend) {
        lock.lock();
        try {
            feedbackQueue.add(sliceExtend);
        } finally {
            lock.unlock();
        }
    }

    /**
     * close slice status feedback thread
     */
    public void stop() {
        this.isCompleted = true;
        this.feedbackSender.shutdownNow();
    }

    /**
     * start slice status feedback thread
     */
    public void feedback() {
        feedbackSender.submit(() -> {
            Thread.currentThread().setName(FEEDBACK_THREAD_NAME);
            SliceExtend sliceExt = null;
            while (!isCompleted) {
                try {
                    sliceExt = feedbackQueue.poll();
                    if (Objects.isNull(sliceExt)) {
                        if (isCompleted) {
                            LogUtils.debug(log, "feedback slice status of is completed");
                        } else {
                            ThreadUtil.sleepOneSecond();
                        }
                    } else {
                        LogUtils.debug(log, "feedback slice status of table [{}]", sliceExt);
                        checkingClient.refreshRegisterSlice(sliceExt);
                    }
                } catch (Exception ex) {
                    LogUtils.error(log, "{}feedback slice status error {}", ErrorCode.FEEDBACK_SLICE_STATUS, sliceExt,
                        ex);
                }
            }
            LogUtils.debug(log, "feedback slice status of is completed and exited");
        });
    }
}
