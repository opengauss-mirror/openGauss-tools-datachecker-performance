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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.data.csv.CsvListener;
import org.opengauss.datachecker.extract.slice.factory.SliceFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.EXTRACT_EXECUTOR;

/**
 * SliceDispatcher
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/21
 * @since ：11
 */
public class SliceDispatcher implements Runnable {
    private static final Logger log = LogUtils.getLogger(SliceDispatcher.class);
    private static final int MAX_DISPATCHER_QUEUE_SIZE = 100;
    private static final int WAIT_ONE_SECOND = 1000;
    private final BlockingQueue<String> tableQueue = new LinkedBlockingQueue<>();
    private final SliceFactory sliceFactory;
    private final CsvListener listener;
    private final BaseDataService baseDataService;
    private final SliceRegister sliceRegister;
    private final DynamicThreadPoolManager dynamicThreadPoolManager;
    private boolean isRunning = true;
    private final int maxFetchSize;

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
        this.maxFetchSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TABLE_SLICE_SIZE);
    }

    @Override
    public void run() {
        try {
            LogUtils.info(log, "slice dispatcher is starting ...");
            synchronized (SliceDispatcher.class) {
                final ThreadPoolExecutor executor = dynamicThreadPoolManager.getExecutor(EXTRACT_EXECUTOR);
                int topicSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TOPIC_SIZE);
                int extendMaxPoolSize = ConfigCache.getIntValue(ConfigConstants.EXTEND_MAXIMUM_POOL_SIZE);
                Endpoint endPoint = ConfigCache.getEndPoint();
                while (isRunning) {
                    waitingForIdle(executor);
                    String table = tableQueue.poll();
                    if (StringUtils.isEmpty(table)) {
                        continue;
                    }
                    // check table by rule of table
                    if (!baseDataService.checkTableContains(table)) {
                        LogUtils.info(log, "distributors ignore [{}] table shards based on table rules", table);
                        notifyIgnoreCsvName(endPoint, table, "tableNoMatch");
                        listener.releaseSliceCache(table);
                        continue;
                    }
                    TableMetadata tableMetadata = baseDataService.queryTableMetadata(table);
                    if (!tableMetadata.hasPrimary()) {
                        LogUtils.info(log, "distributors ignore [{}] table because of it's no primary", table);
                        notifyIgnoreCsvName(endPoint, table, "tableNoPrimary");
                        listener.releaseSliceCache(table);
                        continue;
                    }
                    List<SliceVo> tableSliceList = listener.fetchTableSliceList(table);
                    if (CollectionUtils.isEmpty(tableSliceList)) {
                        LogUtils.warn(log, "table slice is empty, retry to: [{}]", table);
                        tableSliceList = listener.fetchTableSliceList(table);
                        if (CollectionUtils.isEmpty(tableSliceList)) {
                            LogUtils.warn(log, "table slice is empty, ignore: [{}]", table);
                            continue;
                        }
                    }
                    if (tableMetadata.isSinglePrimary()) {
                        doSinglePrimarySliceDispatcher(tableSliceList, tableMetadata, executor, topicSize, extendMaxPoolSize);
                    } else {
                        doMultiPrimarySliceDispatcher(tableSliceList, tableMetadata, executor, topicSize, extendMaxPoolSize);
                    }
                }
                if (listener.isFinished()) {
                    LogUtils.info(log, "listener is finished , and will be closed");
                    listener.stop();
                    while (!dynamicThreadPoolManager.allExecutorFree()) {
                        ThreadUtil.sleepHalfSecond();
                    }
                    stop();
                    dynamicThreadPoolManager.closeDynamicThreadPoolMonitor();
                }
            }
        } catch (Exception exception) {
            LogUtils.error(log, "ex", exception);
        }
    }

    private void doMultiPrimarySliceDispatcher(List<SliceVo> tableSliceList, TableMetadata tableMetadata, ThreadPoolExecutor executor, int topicSize, int extendMaxPoolSize) {
        List<Path> sliceNameList = tableSliceList.stream().map(SliceVo::getName).map(Path::of).collect(Collectors.toList());
        SliceVo multiPkSlice = new SliceVo();
        String table = tableMetadata.getTableName();
        multiPkSlice.setName(table);
        multiPkSlice.setTable(table);
        multiPkSlice.setNo(0);
        multiPkSlice.setTotal(1);
        multiPkSlice.setSchema(tableMetadata.getSchema());
        multiPkSlice.setEndpoint(ConfigCache.getEndPoint());
        multiPkSlice.setStatus(0);
        multiPkSlice.setTableHash(tableMetadata.getTableHash());
        sliceRegister.batchRegister(List.of(multiPkSlice));
        listener.releaseSliceCache(table);
        executor.submit(sliceFactory.createTableProcessor(table, sliceNameList));
    }

    private void doSinglePrimarySliceDispatcher(List<SliceVo> tableSliceList, TableMetadata tableMetadata, ThreadPoolExecutor executor, int topicSize, int extendMaxPoolSize) {
        sliceRegister.batchRegister(tableSliceList);
        String table = tableMetadata.getTableName();
        listener.releaseSliceCache(table);
        if (tableSliceList.size() <= 20) {
            LogUtils.debug(log, "table [{}] get main executor success", table);
            tableSliceList.forEach(sliceVo -> doTableSlice(executor, sliceVo));
        } else {
            ThreadPoolExecutor extendExecutor =
                    (ThreadPoolExecutor) dynamicThreadPoolManager.getFreeExecutor(topicSize, extendMaxPoolSize);
            LogUtils.debug(log, "table [{}] get extend executor success", table);
            tableSliceList.forEach(sliceVo -> doTableSlice(extendExecutor, sliceVo));
        }
    }

    private void notifyIgnoreCsvName(Endpoint endPoint, String ignoreCsvTableName, String reason) {
        listener.notifyCheckIgnoreTable(ignoreCsvTableName, reason);
        if (Objects.equals(Endpoint.SOURCE, endPoint)) {
            String csvDataPath = ConfigCache.getCsvData();
            List<SliceVo> tableSliceList = listener.fetchTableSliceList(ignoreCsvTableName);
            Optional.ofNullable(tableSliceList)
                    .ifPresent(list -> list.forEach(slice -> {
                        String ignoreCsvName = slice.getName();
                        if (FileUtils.renameTo(csvDataPath, ignoreCsvName)) {
                            LogUtils.debug(log, "rename csv sharding completed [{}] by {}", ignoreCsvName, reason);
                        }
                    }));
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
        executor.submit(sliceFactory.createSliceProcessor(sliceVo));
        LogUtils.info(log, "process slice from current queue {}", sliceVo.getName());
    }

    /**
     * if executor has large no running task in the queue, then wait the poll()
     *
     * @param executor executor
     * @throws InterruptedException InterruptedException
     */
    private void waitingForIdle(ThreadPoolExecutor executor) throws InterruptedException {
        BlockingQueue<Runnable> executorQueue = executor.getQueue();
        Thread currentThread = Thread.currentThread();
        while (executorQueue.size() > MAX_DISPATCHER_QUEUE_SIZE) {
            currentThread.wait(WAIT_ONE_SECOND);
        }
    }

    /**
     * stop slice dispatcher core thread
     */
    public void stop() {
        isRunning = false;
    }

    /**
     * addSliceTables
     *
     * @param list list
     */
    public void addSliceTables(List<String> list) {
        if (CollectionUtils.isNotEmpty(list)) {
            tableQueue.addAll(list);
        }
    }
}