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
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.MapUtils;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.data.csv.CsvListener;
import org.opengauss.datachecker.extract.slice.factory.SliceFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.EXTRACT_EXECUTOR;

/**
 * SliceDispatcher
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/21
 * @since ：11
 */
public class SliceDispatcher implements Runnable {
    private static final Logger log = LogUtils.getLogger();
    private static final int LOG_PERIOD = 60;
    private static final int MAX_DISPATCHER_QUEUE_SIZE = 100;
    private static final int WAIT_ONE_SECOND = 1000;

    private static final Map<String, List<SliceVo>> unprocessedTableSlices = new ConcurrentHashMap<>();

    private final Object lock = new Object();
    private final SliceFactory sliceFactory;
    private final CsvListener listener;
    private final BaseDataService baseDataService;
    private final SliceRegister sliceRegister;
    private final DynamicThreadPoolManager dynamicThreadPoolManager;

    private boolean isRunning = true;
    private int waitTimes = 0;

    /**
     * construct slice dispatcher
     *
     * @param dynamicThreadPoolManager dynamicThreadPoolManager
     * @param sliceRegister            sliceRegister
     * @param baseDataService          baseDataService
     * @param listener                 listener
     */
    public SliceDispatcher(DynamicThreadPoolManager dynamicThreadPoolManager, SliceRegister sliceRegister,
        BaseDataService baseDataService, CsvListener listener) {
        this.listener = listener;
        this.sliceRegister = sliceRegister;
        this.baseDataService = baseDataService;
        this.dynamicThreadPoolManager = dynamicThreadPoolManager;
        this.sliceFactory = new SliceFactory(baseDataService.getDataSource());
    }

    @Override
    public void run() {
        try {
            log.info("slice dispatcher is starting ...");
            synchronized (lock) {
                final ThreadPoolExecutor executor = dynamicThreadPoolManager.getExecutor(EXTRACT_EXECUTOR);
                Endpoint endPoint = ConfigCache.getEndPoint();
                while (isRunning) {
                    waitingForIdle(executor);
                    SliceVo sliceVo = listener.poll();
                    waitingForNewSlice(sliceVo);
                    if (Objects.nonNull(sliceVo)) {
                        // check table by rule of table
                        if (!baseDataService.checkTableContains(sliceVo.getTable())) {
                            log.info("slice dispatcher is ignoring slice of {}", sliceVo.getTable());
                            continue;
                        }
                        sliceVo.setEndpoint(endPoint);
                        register(sliceVo);
                        doTableSlice(executor, sliceVo);
                    } else {
                        batchUnprocessedTableSlices(executor);
                    }
                    if (listener.isFinished()) {
                        listener.stop();
                        while (executor.getTaskCount() == executor.getCompletedTaskCount()) {
                            sliceRegister.notifyDispatchCsvSliceFinished();
                            stop();
                            dynamicThreadPoolManager.closeDynamicThreadPoolMonitor();
                        }
                    }
                }
            }
        } catch (Exception exception) {
            log.error("ex", exception);
        }
    }

    /**
     * check current slice whether registered topic ,
     * if true , then add slice to the executor,
     * and check  unprocessedTableSlices whether had current slice table's unprocessed slices.
     * if unprocessedTableSlices has, do it.
     *
     * @param executor executor
     * @param sliceVo  sliceVo
     */
    private void doTableSlice(ThreadPoolExecutor executor, SliceVo sliceVo) {
        String table = sliceVo.getTable();
        if (checkTopicRegistered(table)) {
            if (unprocessedTableSlices.containsKey(table)) {
                List<SliceVo> unprocessedSlices = unprocessedTableSlices.get(table);
                doTableUnprocessedSlices(executor, table, unprocessedSlices);
            }
            executor.submit(sliceFactory.createSliceProcessor(sliceVo));
        }
    }

    /**
     * if we poll from listener is null, it means listener queue is empty. we have no more slice to do.
     * so , we batch process the unprocessedTableSlices .
     *
     * @param executor executor
     */
    private void batchUnprocessedTableSlices(ThreadPoolExecutor executor) {
        unprocessedTableSlices.forEach((table, unprocessedSlices) -> {
            if (checkTopicRegistered(table)) {
                doTableUnprocessedSlices(executor, table, unprocessedSlices);
            }
        });
    }

    private void doTableUnprocessedSlices(ThreadPoolExecutor executor, String table, List<SliceVo> unprocessedSlices) {
        List<SliceVo> processedSlices = new LinkedList<>();
        unprocessedSlices.forEach(unprocessed -> {
            executor.submit(sliceFactory.createSliceProcessor(unprocessed));
            processedSlices.add(unprocessed);
        });
        processedSlices.forEach(processed -> MapUtils.remove(unprocessedTableSlices, table, processed));
        processedSlices.clear();
    }

    /**
     * if current poll slice is null ,and  unprocessedTableSlices queue is empty, so we wait current thread.
     *
     * @param sliceVo sliceVo
     * @throws InterruptedException InterruptedException
     */
    private void waitingForNewSlice(SliceVo sliceVo) throws InterruptedException {
        synchronized (lock) {
            if (Objects.isNull(sliceVo) && unprocessedTableSlices.isEmpty()) {
                waitTimes++;
                if (waitTimes % LOG_PERIOD == 0) {
                    log.info("slice dispatcher is waiting for slice task from {}", listener.getClass().getSimpleName());
                    waitTimes = 0;
                }
                lock.wait(WAIT_ONE_SECOND);
            }
        }
    }

    /**
     * if executor has large no running task in the queue, then wait the poll()
     *
     * @param executor executor
     * @throws InterruptedException InterruptedException
     */
    private void waitingForIdle(ThreadPoolExecutor executor) throws InterruptedException {
        synchronized (lock) {
            while (executor.getQueue().size() > MAX_DISPATCHER_QUEUE_SIZE) {
                lock.wait(WAIT_ONE_SECOND);
            }
        }
    }

    private boolean checkTopicRegistered(String table) {
        return sliceRegister.checkTopicRegistered(table);
    }

    /**
     * register slice to check server;
     * we use the slice table ,try to register kafka topic.
     * if register topic success, return and add current slice in executor queue.
     * if register not, add current slice in unprocessedTableSlices queue.
     *
     * @param sliceVo sliceVo
     */
    private void register(SliceVo sliceVo) {
        sliceRegister.register(sliceVo);
        if (sliceVo.getPtnNum() > 0 && !sliceRegister.registerTopic(sliceVo.getTable(), sliceVo.getPtnNum())) {
            MapUtils.put(unprocessedTableSlices, sliceVo.getTable(), sliceVo);
        }
    }

    /**
     * stop slice dispatcher core thread
     */
    public void stop() {
        isRunning = false;
    }
}