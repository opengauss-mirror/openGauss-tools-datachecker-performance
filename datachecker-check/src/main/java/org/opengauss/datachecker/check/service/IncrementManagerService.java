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

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.IncrementCheckProperties;
import org.opengauss.datachecker.check.modules.check.DataCheckService;
import org.opengauss.datachecker.check.modules.check.ExportCheckResult;
import org.opengauss.datachecker.check.modules.report.CheckResultManagerService;
import org.opengauss.datachecker.check.modules.report.SliceProgressService;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.report.CheckFailed;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.PhaserUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import javax.annotation.Resource;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.COMMA;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.FAILED_LOG_NAME;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.LEFT_SQUARE_BRACKET;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.RIGHT_SQUARE_BRACKET;

/**
 * IncrementManagerService
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Service
public class IncrementManagerService {
    private static final Logger log = LogUtils.getLogger(IncrementManagerService.class);
    private static final AtomicReference<String> PROCESS_SIGNATURE = new AtomicReference<>();
    private static final BlockingQueue<List<SourceDataLog>> INC_LOG_QUEUE = new LinkedBlockingQueue<>();
    private static final int RETRY_SLEEP_TIMES = 1000;
    private static final int MAX_RETRY_SLEEP_TIMES = 1000;

    @Resource
    private DataCheckService dataCheckService;
    @Resource
    private SliceProgressService sliceProgressService;
    @Resource
    private FeignClientService feignClientService;
    @Resource
    private IncrementCheckProperties properties;
    @Resource
    private ShutdownService shutdownService;
    @Resource
    private CheckResultManagerService checkResultManagerService;
    @Resource
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    private final AtomicInteger retryTimes = new AtomicInteger(0);
    private final AtomicInteger totalKeySize = new AtomicInteger(0);

    /**
     * Incremental verification log notification
     *
     * @param dataLogList Incremental verification log
     */
    public void notifySourceIncrementDataLogs(List<SourceDataLog> dataLogList) {
        if (CollectionUtils.isEmpty(dataLogList)) {
            return;
        }
        int sum = dataLogList.stream().map(SourceDataLog::length).mapToInt(Integer::intValue).sum();
        totalKeySize.addAndGet(sum);
        try {
            INC_LOG_QUEUE.put(dataLogList);
            LogUtils.info(log, "increment log queue size= {}, current add {} tables, contain : {}",
                    INC_LOG_QUEUE.size(), dataLogList.size(), getDataLogTables(dataLogList));
        } catch (InterruptedException ex) {
            LogUtils.error(log, "notify inc data logs interrupted  ");
        }
    }

    private List<String> getDataLogTables(List<SourceDataLog> dataLogList) {
        return dataLogList.stream()
                .map(SourceDataLog::getTableName)
                .collect(Collectors.toList());
    }

    public void startIncrementDataLogs() {
        if (feignClientService.startIncrementMonitor()) {
            LogUtils.info(log, "started source increment monitor");
            feignClientService.resumeIncrementMonitor();
            ThreadUtil.newSingleThreadExecutor()
                    .submit(this::checkingIncrementDataLogs);
        } else {
            throw new CheckingException("start increment monitor failed");
        }
    }

    private void checkingIncrementDataLogs() {
        Thread.currentThread()
                .setName("inc-queue-process-loop");
        LogUtils.info(log, "started process increment data logs thread");
        shutdownService.addMonitor();
        while (!shutdownService.isShutdown()) {
            consumerIncLogQueue();
        }
        shutdownService.releaseMonitor();
    }

    private void consumerIncLogQueue() {
        try {
            final List<SourceDataLog> dataLogList = new LinkedList<>();
            final Map<String, SourceDataLog> lastResults = collectLastResults();
            int diffCount = calculationLastResultDiffCount(lastResults);
            if (diffCount >= properties.getIncrementMaxDiffCount()) {
                feignClientService.pauseIncrementMonitor();
                LogUtils.warn(log, "pause increment monitor, because the diff-count is too large [{}] ,"
                        + " please to repair this large diff manual !", diffCount);
                ThreadUtil.sleep(getRetryTime());
            }
            if (INC_LOG_QUEUE.isEmpty()) {
                feignClientService.resumeIncrementMonitor();
                LogUtils.warn(log, "resume increment monitor, because the inc-log-queue is empty !");
            } else {
                feignClientService.pauseIncrementMonitor();
                LogUtils.warn(log, "pause increment monitor, because the inc-log-queue is not empty !");
                dataLogList.addAll(INC_LOG_QUEUE.take());
            }
            // Collect the last verification results and build an incremental verification log
            mergeDataLogList(dataLogList, lastResults);
            if (CollectionUtils.isNotEmpty(dataLogList)) {
                ExportCheckResult.backCheckResultDirectory();
                sliceProgressService.startProgressing();
                PROCESS_SIGNATURE.set(IdGenerator.nextId36());
                checkResultManagerService.progressing(dataLogList.size());
                incrementDataLogsChecking(dataLogList);
                feignClientService.resumeIncrementMonitor();
            } else {
                ThreadUtil.sleepSecond(1);
            }
        } catch (Exception ex) {
            LogUtils.error(log, "take inc log queue interrupted  ");
        }
    }

    private int getRetryTime() {
        if (retryTimes.get() > MAX_RETRY_SLEEP_TIMES) {
            retryTimes.set(1);
        }
        return retryTimes.incrementAndGet() * RETRY_SLEEP_TIMES;
    }

    private int calculationLastResultDiffCount(Map<String, SourceDataLog> lastResults) {
        AtomicInteger diffCount = new AtomicInteger();
        lastResults.forEach((tableName, lastLog) -> {
            diffCount.addAndGet(lastLog.getCompositePrimaryValues()
                    .size());
        });
        return diffCount.get();
    }

    private void mergeDataLogList(List<SourceDataLog> dataLogList, Map<String, SourceDataLog> collectLastResults) {
        final Map<String, SourceDataLog> dataLogMap = dataLogList.stream()
                .collect(Collectors.toMap(SourceDataLog::getTableName,
                        Function.identity()));
        collectLastResults.forEach((tableName, lastLog) -> {
            if (dataLogMap.containsKey(tableName)) {
                final List<String> values = dataLogMap.get(tableName)
                        .getCompositePrimaryValues();
                final long beginOffset = Math.min(dataLogMap.get(tableName)
                        .getBeginOffset(), lastLog.getBeginOffset());
                final Set<String> margeValueSet = new HashSet<>();
                margeValueSet.addAll(values);
                margeValueSet.addAll(lastLog.getCompositePrimaryValues());
                dataLogMap.get(tableName)
                        .getCompositePrimaryValues()
                        .clear();
                dataLogMap.get(tableName)
                        .setBeginOffset(beginOffset);
                dataLogMap.get(tableName)
                        .getCompositePrimaryValues()
                        .addAll(margeValueSet);
            } else {
                dataLogList.add(lastLog);
            }
        });
        LogUtils.debug(log, "merge last failed results {}", collectLastResults.keySet());
    }

    private void incrementDataLogsChecking(List<SourceDataLog> dataLogList) {
        String processNo = PROCESS_SIGNATURE.get();
        List<Runnable> taskList = new ArrayList<>();
        StopWatch stopWatch = new StopWatch("check inc data " + processNo);
        stopWatch.start("start check thread");
        dataLogList.forEach(dataLog -> {
            LogUtils.debug(log, "increment process=[{}] , tableName=[{}], begin offset =[{}], diffSize=[{}]", processNo,
                    dataLog.getTableName(), dataLog.getBeginOffset(), dataLog.getCompositePrimaryValues()
                            .size());
            // Verify the data according to the table name and Kafka partition
            taskList.add(dataCheckService.incrementCheckTableData(dataLog.getTableName(), processNo, dataLog));
        });
        stopWatch.stop();
        stopWatch.start("wait check thread");
        // Block the current thread until all thread pool tasks are executed.
        PhaserUtil.submit(threadPoolTaskExecutor, taskList, () -> {
            LogUtils.debug(log, "multiple check subtasks have been completed.");
        });
        stopWatch.stop();
        stopWatch.start("summary check result");
        checkResultManagerService.summaryCheckResult();
        stopWatch.stop();
        LogUtils.debug(log, stopWatch.prettyPrint());
    }

    /**
     * Collect the last verification results and build an incremental verification log
     *
     * @return Analysis of last verification result
     */
    private Map<String, SourceDataLog> collectLastResults() {
        final List<Path> checkResultFileList = FileUtils.loadDirectory(ExportCheckResult.getResultPath());
        LogUtils.debug(log, "collect last results path {}", checkResultFileList);
        if (CollectionUtils.isEmpty(checkResultFileList)) {
            return new HashMap<>();
        }
        List<CheckFailed> historyFailedList = new ArrayList<>();
        checkResultFileList.stream()
                .filter(path -> FAILED_LOG_NAME.equals(path.getFileName()
                        .toString()))
                .forEach(path -> {
                    try {
                        String content = wrapperFailedResultArray(path);
                        historyFailedList.addAll(JSONObject.parseArray(content, CheckFailed.class));
                    } catch (CheckingException | JSONException ex) {
                        log.error("load check result {} has error", path.getFileName());
                    }
                });

        LogUtils.debug(log, "collect last failed results {}", historyFailedList.size());
        return parseCheckResult(historyFailedList);
    }

    private String wrapperFailedResultArray(Path path) {
        final String contents = FileUtils.readFileContents(path);
        if (StringUtils.isEmpty(contents)) {
            return contents;
        }
        StringBuilder sb = new StringBuilder(LEFT_SQUARE_BRACKET);
        sb.append(contents);
        if (contents.endsWith(COMMA)) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(RIGHT_SQUARE_BRACKET);
        return sb.toString();
    }

    private Map<String, SourceDataLog> parseCheckResult(List<CheckFailed> historyDataList) {
        Map<String, SourceDataLog> dataLogMap = new HashMap<>();
        historyDataList.forEach(dataLog -> {
            final Set<String> diffKeyValues = getDiffKeyValues(dataLog);
            final String tableName = dataLog.getTable();
            if (dataLogMap.containsKey(tableName)) {
                final SourceDataLog dataLogMarge = dataLogMap.get(tableName);
                final long beginOffset = Math.min(dataLogMarge.getBeginOffset(), dataLog.getBeginOffset());
                final List<String> values = dataLogMarge.getCompositePrimaryValues();
                diffKeyValues.addAll(values);
                dataLogMarge.getCompositePrimaryValues()
                        .clear();
                dataLogMarge.getCompositePrimaryValues()
                        .addAll(diffKeyValues);
                dataLogMarge.setBeginOffset(beginOffset);
            } else {
                SourceDataLog sourceDataLog = new SourceDataLog();
                sourceDataLog.setTableName(tableName)
                        .setBeginOffset(dataLog.getBeginOffset())
                        .setCompositePrimaryValues(new ArrayList<>(diffKeyValues));
                dataLogMap.put(tableName, sourceDataLog);
            }
        });
        LogUtils.debug(log, "parse last failed results {}", dataLogMap.keySet());
        return dataLogMap;
    }

    private Set<String> getDiffKeyValues(CheckFailed dataLog) {
        Set<String> keyValues = new HashSet<>();
        keyValues.addAll(dataLog.getKeyInsertSet());
        keyValues.addAll(dataLog.getKeyUpdateSet());
        keyValues.addAll(dataLog.getKeyDeleteSet());
        return keyValues;
    }
}
