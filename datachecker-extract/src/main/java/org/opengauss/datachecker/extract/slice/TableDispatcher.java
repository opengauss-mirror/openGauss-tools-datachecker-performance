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
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.slice.common.CsvDataFileScanner;
import org.opengauss.datachecker.extract.slice.factory.SliceFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.EXTRACT_EXECUTOR;

/**
 * TableDispatcher
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/21
 * @since ：11
 */
public class TableDispatcher implements Runnable {
    private static final Logger log = LogUtils.getLogger();
    private static final int MAX_DISPATCHER_QUEUE_SIZE = 500;
    private static final int WAIT_ONE_SECOND = 1000;

    private final Object lock = new Object();
    private final Object lockIdle = new Object();
    private final SliceFactory sliceFactory;
    private final BaseDataService baseDataService;
    private final SliceRegister sliceRegister;
    private final DynamicThreadPoolManager dynamicThreadPoolManager;

    private boolean isRunning = true;

    /**
     * construct slice dispatcher
     *
     * @param dynamicThreadPoolManager dynamicThreadPoolManager
     * @param sliceRegister            sliceRegister
     * @param baseDataService          baseDataService
     */
    public TableDispatcher(DynamicThreadPoolManager dynamicThreadPoolManager, SliceRegister sliceRegister,
        BaseDataService baseDataService) {
        this.sliceRegister = sliceRegister;
        this.baseDataService = baseDataService;
        this.dynamicThreadPoolManager = dynamicThreadPoolManager;
        this.sliceFactory = new SliceFactory(baseDataService.getDataSource());
    }

    @Override
    public void run() {
        try {
            log.info("table dispatcher is starting ...");
            synchronized (lock) {
                final ThreadPoolExecutor executor = dynamicThreadPoolManager.getExecutor(EXTRACT_EXECUTOR);
                List<String> tableNameList = baseDataService.bdsQueryTableNameList();
                if (CollectionUtils.isEmpty(tableNameList)) {
                    log.info("table dispatcher load tables empty , is ending ...");
                    return;
                }
                log.info("table dispatcher load {} tables , is ending ...", tableNameList.size());
                tableNameList.sort(String::compareTo);

                if (Objects.equals(Endpoint.SOURCE, ConfigCache.getEndPoint())) {
                    log.info("table dispatcher load path {} ", ConfigCache.getCsvMetadataTablesPath());
                    log.info("table dispatcher load path {} ", ConfigCache.getCsvMetadataColumnsPath());
                    log.info("table dispatcher load path {} ", ConfigCache.getCsvData());
                    CsvDataFileScanner scanner = new CsvDataFileScanner(ConfigCache.getSchema(), tableNameList);
                    scanner.scanCsvFile(Path.of(ConfigCache.getCsvData()));
                    tableNameList.forEach(table -> {
                        waitingForIdle(executor);
                        register(buildTableSlice(table));
                        doTableSlice(executor, table, scanner.getTablePaths(table));
                    });

                } else {
                    tableNameList.forEach(table -> {
                        waitingForIdle(executor);
                        register(buildTableSlice(table));
                        doTableSlice(executor, table);
                    });
                }

                while (isRunning) {
                    waitingForIdle(executor);
                    if (executor.getTaskCount() == executor.getCompletedTaskCount()) {
                        stop();
                        dynamicThreadPoolManager.closeDynamicThreadPoolMonitor();
                    }
                }
            }
        } catch (Exception exception) {
            log.error("ex", exception);
        }
    }

    private SliceVo buildTableSlice(String table) {
        TableMetadata tableMetadata = baseDataService.queryTableMetadata(table);
        SliceVo tableSlice = new SliceVo();
        tableSlice.setSchema(ConfigCache.getSchema());
        tableSlice.setTable(table);
        tableSlice.setName(table);
        tableSlice.setWholeTable(true);
        tableSlice.setTotal(1);
        tableSlice.setEndpoint(ConfigCache.getEndPoint());
        tableSlice.setTableHash(tableMetadata.getTableHash());
        tableSlice.setPtnNum(1);
        return tableSlice;
    }

    /**
     * check current slice whether registered topic ,
     * if true , then add slice to the executor,
     * and check  unprocessedTableSlices whether had current slice table's unprocessed slices.
     * if unprocessedTableSlices has, do it.
     *
     * @param executor       executor
     * @param table          table
     * @param tableFilePaths tableFilePaths
     */
    private void doTableSlice(ThreadPoolExecutor executor, String table, List<Path> tableFilePaths) {
        executor.submit(sliceFactory.createTableProcessor(table, tableFilePaths));
    }

    private void doTableSlice(ThreadPoolExecutor executor, String table) {
        doTableSlice(executor, table, null);
    }

    /**
     * if executor has large no running task in the queue, then wait the poll()
     *
     * @param executor executor
     */
    private void waitingForIdle(ThreadPoolExecutor executor) {
        synchronized (lockIdle) {
            while (executor.getQueue()
                           .size() > MAX_DISPATCHER_QUEUE_SIZE) {
                try {
                    lock.wait(WAIT_ONE_SECOND);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    /**
     * register slice to check server;
     *
     * @param sliceVo sliceVo
     */
    private void register(SliceVo sliceVo) {
        sliceRegister.batchRegister(List.of(sliceVo));
    }

    /**
     * stop slice dispatcher core thread
     */
    public void stop() {
        isRunning = false;
    }
}