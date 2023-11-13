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

import feign.RetryableException;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.ExtractSinkFeignClient;
import org.opengauss.datachecker.check.client.ExtractSourceFeignClient;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.thread.CheckUncaughtExceptionHandler;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * CheckPointRegister
 *
 * @author ：lvlintao
 * @date ：Created in 2023/10/28
 * @since ：11
 */
@Component
public class CheckPointRegister {
    /**
     * table checkpoint map
     */
    protected static final Map<String, List<Object>> checkPointCounter = new ConcurrentHashMap<>();
    private static final Logger log = LogUtils.getLogger();

    @Autowired
    private ExtractSourceFeignClient extractSourceClient;
    @Autowired
    private ExtractSinkFeignClient extractSinkClient;

    private final ReentrantLock lock = new ReentrantLock();
    private final BlockingQueue<CheckPointData> checkPointQueue = new LinkedBlockingQueue<>();
    private CheckPointMonitor checkPointMonitor;
    private boolean isSinkStop;
    private boolean isSourceStop;

    /**
     * register table checkPoint list
     *
     * @param endpoint       endpoint
     * @param tableName      tableName
     * @param checkPointList checkPointList
     */
    public void registerCheckPoint(Endpoint endpoint, String tableName, List<Object> checkPointList) {
        lock.lock();
        try {
            log.info("{} [{}] register checkpoint, current checkpoint size is [{}]", endpoint, tableName,
                checkPointList.size());
            if (checkPointCounter.containsKey(tableName)) {
                List<Object> preCheckPoint = checkPointCounter.get(tableName);
                CheckPointData checkPointData = calculateCheckPoint(tableName, preCheckPoint, checkPointList);
                log.info("{} [{}] register checkpoint, calculated checkpoint size is [{}]", endpoint, tableName,
                    checkPointData.getCheckPointList()
                                  .size());
                checkPointQueue.add(checkPointData);
            } else {
                checkPointCounter.put(tableName, checkPointList);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * start table checkPoint monitor
     */
    public synchronized void startMonitor() {
        if (Objects.isNull(checkPointMonitor)) {
            checkPointMonitor = new CheckPointMonitor(checkPointQueue);
            Thread thread = new Thread(checkPointMonitor);
            thread.setUncaughtExceptionHandler(new CheckUncaughtExceptionHandler(log));
            thread.start();
        }
    }

    /**
     * stop the table checkPoint monitor
     *
     * @param endpoint endpoint
     */
    public void stopMonitor(Endpoint endpoint) {
        if (Objects.equals(endpoint, Endpoint.SOURCE)) {
            this.isSourceStop = true;
        }
        if (Objects.equals(endpoint, Endpoint.SINK)) {
            this.isSinkStop = true;
        }
        if (isSourceStop && isSinkStop) {
            checkPointMonitor.stop();
        }
    }

    private CheckPointData calculateCheckPoint(String tableName, List<Object> checkPointList, List<Object> pointList) {
        CheckPointData checkPointData = new CheckPointData();
        List<Object> unionCheckPoint = new ArrayList<>() {{
            addAll(checkPointList);
            addAll(pointList);
        }};
        List<Object> calculatedCheckpoint = unionCheckPoint.stream()
                                                           .distinct()
                                                           .sorted()
                                                           .collect(Collectors.toList());
        if (!calculatedCheckpoint.isEmpty()) {
            checkPointList.set(0, calculatedCheckpoint.get(0));
            checkPointList.set(checkPointList.size() - 1, calculatedCheckpoint.get(calculatedCheckpoint.size() - 1));
        }
        return checkPointData.setTableName(tableName)
                             .setCheckPointList(checkPointList);
    }

    @Data
    @Accessors(chain = true)
    class CheckPointData {
        private String tableName;
        private List<Object> checkPointList;
    }

    /**
     * In the check, the checkPoint monitor is use to send calculated table checkpoint to source and sink
     */
    public class CheckPointMonitor implements Runnable {
        private final BlockingQueue<CheckPointData> queue;
        private boolean isStop;

        /**
         * construct the checkPoint monitor
         *
         * @param checkPointQueue checkPointQueue
         */
        public CheckPointMonitor(BlockingQueue<CheckPointData> checkPointQueue) {
            this.queue = checkPointQueue;
            this.isStop = false;
        }

        @Override
        public void run() {
            int deliveredCount = 0;
            log.info("check point monitor start");
            while (!isStop) {
                try {
                    if (queue.isEmpty()) {
                        ThreadUtil.sleep(100);
                    }
                    CheckPointData curCheckPointData = queue.take();
                    sendCheckPoint(curCheckPointData.getTableName(), curCheckPointData.getCheckPointList());
                    deliveredCount++;
                    log.info("[{}][{}] table checkpoint list send success", deliveredCount,
                        curCheckPointData.getTableName());
                } catch (InterruptedException exp) {
                    log.warn("send checkpoint occur interrupt exception", exp);
                } catch (RetryableException exp) {
                    log.warn("send checkpoint occur retryable exception", exp);
                }
            }
            log.info("check point monitor stopped");
        }

        /**
         * refresh extract table checkPoint list
         *
         * @param tableName      tableName
         * @param checkPointList checkPointList
         */
        @Retryable(maxAttempts = 3)
        public void sendCheckPoint(String tableName, List<Object> checkPointList) {
            extractSourceClient.refreshCheckpoint(tableName, checkPointList);
            ThreadUtil.sleepHalfSecond();
            extractSinkClient.refreshCheckpoint(tableName, checkPointList);
        }

        /**
         * stop the monitor
         */
        public void stop() {
            this.isStop = true;
        }
    }
}
