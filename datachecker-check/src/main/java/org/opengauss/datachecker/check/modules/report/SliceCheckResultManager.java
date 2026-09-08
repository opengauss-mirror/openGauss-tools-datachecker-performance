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

package org.opengauss.datachecker.check.modules.report;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.CheckResultConstants;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.check.CheckTableInfo;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.Database;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.report.CheckCsvFailed;
import org.opengauss.datachecker.common.entry.report.CheckFailed;
import org.opengauss.datachecker.common.entry.report.CheckSuccess;
import org.opengauss.datachecker.common.entry.report.CheckSummary;
import org.opengauss.datachecker.common.util.CheckResultUtils;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import jakarta.validation.constraints.NotEmpty;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.FAILED_MESSAGE;

/**
 * SliceCheckResultManager
 *
 * @author ：wangchao
 * @date ：Created in 2023/2/24
 * @since ：11
 */
@Service
public class SliceCheckResultManager {
    private static final Logger log = LogUtils.getLogger(SliceCheckResultManager.class);
    private static final String SUMMARY_LOG_NAME = "summary.log";
    private static final String SUCCESS_LOG_NAME = "success.log";
    private static final String REPAIR_LOG_TEMPLATE = "repair_%s_%s_%s.txt";
    private static final int MAX_DISPLAY_SIZE = CheckResultConstants.MAX_DISPLAY_SIZE;
    private static final int MAX_SUCCESS_MESSAGE_SIZE = 20;

    private final Map<String, Integer> tableSliceCountMap = new ConcurrentHashMap<>();
    private final Map<String, List<CheckDiffResult>> checkResult = new ConcurrentHashMap<>();
    private final Map<String, CheckDiffResult> tableStructureResult = new ConcurrentHashMap<>();

    @Resource
    private FeignClientService feignClient;
    private final AtomicInteger successTableCount = new AtomicInteger(0);
    private final AtomicInteger failedTableCount = new AtomicInteger(0);
    private final AtomicLong rowCount = new AtomicLong(0L);
    private volatile boolean isCsvMode = false;
    private volatile boolean isOgCompatibility = false;
    private final AtomicBoolean hasInitSliceResultEnvironment = new AtomicBoolean(true);
    private volatile CheckTableInfo checkTableInfo = null;

    /**
     * add slice check result
     *
     * @param slice slice
     * @param result result
     */
    public void addResult(SliceVo slice, CheckDiffResult result) {
        if (!tableSliceCountMap.containsKey(slice.getTable())) {
            tableSliceCountMap.put(slice.getTable(), slice.getTotal());
        }
        if (hasInitSliceResultEnvironment.compareAndSet(true, false)) {
            isCsvMode = Objects.equals(ConfigCache.getCheckMode(), CheckMode.CSV);
            isOgCompatibility = feignClient.checkTargetOgCompatibility();
            LogUtils.info(log, "init slice result environment: isCsvMode={}, ogCompatibility={}", isCsvMode,
                    isOgCompatibility);
        }
        addResult(slice.getTable(), result);
    }

    public void addTableStructureDiffResult(SliceVo slice, CheckDiffResult result) {
        addTableStructureDiffResult(slice.getTable(), result);
    }

    /**
     * add table structure diff result
     *
     * @param table table
     * @param result result
     */
    public void addTableStructureDiffResult(String table, CheckDiffResult result) {
        if (tableStructureResult.containsKey(table)) {
            return;
        }
        log.error("{}checked table structure failed, [{}]", ErrorCode.TABLE_STRUCTURE, table);
        tableStructureResult.put(table, result);
        failedTableCount.incrementAndGet();
        CheckFailed failed = translateCheckFailed(List.of(), List.of(result));
        String failedLogPath = ConfigCache.getCheckResult() + CheckResultConstants.FAILED_LOG_NAME;
        FileUtils.writeAppendFile(failedLogPath, JsonObjectUtil.prettyFormatMillis(failed) + ",");
        refreshSummary();
    }

    /**
     * add table check result
     *
     * @param table table name
     * @param checkDiffResult check result
     */
    public void addResult(String table, CheckDiffResult checkDiffResult) {
        AtomicInteger completedSliceSize = new AtomicInteger();
        checkResult.compute(table, (key, listValue) -> {
            if (listValue == null) {
                listValue = new LinkedList<>();
            }
            listValue.add(checkDiffResult);
            completedSliceSize.set(listValue.size());
            return listValue;
        });
        // is Immediately Rename Check Success File
        boolean isImmediately = Objects.equals(checkDiffResult.getResult(), CheckResultConstants.RESULT_SUCCESS);
        notifyCsvShardingCompleted(checkDiffResult, isImmediately);
        Integer totalSlice = tableSliceCountMap.get(table);
        if (totalSlice != null && completedSliceSize.get() == totalSlice) {
            List<CheckDiffResult> results = checkResult.get(table);
            Map<String, List<CheckDiffResult>> resultMap;
            resultMap = results.stream().collect(Collectors.groupingBy(CheckDiffResult::getResult));
            String checkResultPath = ConfigCache.getCheckResult();
            if (resultMap.containsKey(CheckResultConstants.RESULT_FAILED)) {
                List<CheckDiffResult> tableFailedList = resultMap.get(CheckResultConstants.RESULT_FAILED);
                List<CheckDiffResult> tableSuccessList = resultMap.get(CheckResultConstants.RESULT_SUCCESS);
                CheckFailed failed = translateCheckFailed(tableSuccessList, tableFailedList);
                String failedLogPath = checkResultPath + CheckResultConstants.FAILED_LOG_NAME;
                FileUtils.writeAppendFile(failedLogPath, JsonObjectUtil.prettyFormatMillis(failed) + ",");
                reduceFailedRepair(checkResultPath, tableFailedList);
                if (isCsvMode) {
                    List<CheckCsvFailed> csvFailedList = translateCheckCsvFaileds(results);
                    String csvFailedLogPath = checkResultPath + CheckResultConstants.CSV_FAILED_DETAIL_NAME;
                    saveCsvSliceFailedDetails(csvFailedList, csvFailedLogPath);
                }
                rowCount.addAndGet(failed.getRowCount());
                failedTableCount.incrementAndGet();
            } else {
                List<CheckDiffResult> tableSuccessList = resultMap.get(CheckResultConstants.RESULT_SUCCESS);
                CheckSuccess success = translateCheckSuccess(tableSuccessList);
                String successLogPath = checkResultPath + SUCCESS_LOG_NAME;
                FileUtils.writeAppendFile(successLogPath, JsonObjectUtil.prettyFormatMillis(success) + ",");
                successTableCount.incrementAndGet();
                rowCount.addAndGet(success.getRowCount());
            }
            refreshSummary();
        }
    }

    private void saveCsvSliceFailedDetails(List<CheckCsvFailed> csvFailedList, String csvFailedLogPath) {
        if (CollectionUtils.isEmpty(csvFailedList)) {
            return;
        }
        csvFailedList.stream().filter(CheckCsvFailed::isNotEmpty).forEach(csvFailed -> {
            FileUtils.writeAppendFile(csvFailedLogPath, JsonObjectUtil.formatSimple(csvFailed) + ",");
            FileUtils.writeAppendFile(csvFailedLogPath, System.lineSeparator());
        });
    }

    private void notifyCsvShardingCompleted(CheckDiffResult checkDiffResult, boolean immediately) {
        if (isCsvMode && immediately) {
            String csvDataPath = ConfigCache.getCsvData();
            if (FileUtils.renameTo(csvDataPath, checkDiffResult.getFileName())) {
                LogUtils.info(log, "rename csv sharding completed [{}]", checkDiffResult.getFileName());
            } else {
                LogUtils.warn(log, "rename csv sharding false [{}]", checkDiffResult.getFileName());
            }
        }
    }

    private List<CheckCsvFailed> translateCheckCsvFaileds(List<CheckDiffResult> results) {
        return results.stream()
            .map(result -> new CheckCsvFailed().setTable(result.getTable())
                .build(result.getFileName(), result.getKeyInsert(), result.getKeyUpdate(), result.getKeyDelete()))
            .collect(Collectors.toList());
    }

    /**
     * refresh summary log
     */
    public void refreshSummary() {
        CheckSummary checkSummary = new CheckSummary();
        int completeCount = successTableCount.get() + failedTableCount.get();
        checkSummary.setMode(ConfigCache.getCheckMode());
        checkSummary.setTableCount(completeCount);
        checkSummary.setMissTable(checkTableInfo);
        checkSummary.setStartTime(ConfigCache.getValue(ConfigConstants.START_LOCAL_TIME, LocalDateTime.class));
        checkSummary.setEndTime(LocalDateTime.now());
        checkSummary.setCost(calcCheckTaskCost(checkSummary.getStartTime(), checkSummary.getEndTime()));
        checkSummary.setSuccessCount(successTableCount.get());
        checkSummary.setFailedCount(failedTableCount.get());
        checkSummary.setRowCount(rowCount.get());
        String summaryPath = ConfigCache.getCheckResult() + SUMMARY_LOG_NAME;
        FileUtils.writeFile(summaryPath, JsonObjectUtil.prettyFormatMillis(checkSummary));
    }

    private CheckFailed translateCheckFailed(List<CheckDiffResult> successList, List<CheckDiffResult> tableFailedList) {
        CheckFailed failed = new CheckFailed();
        CheckDiffResult resultCommon = tableFailedList.get(0);
        BeanUtils.copyProperties(resultCommon, failed);
        StringBuilder hasMore = new StringBuilder();
        Set<String> insertKeySet = fetchInsertDiffKeys.fetchKey(tableFailedList);
        Set<String> deleteKeySet = fetchDeleteDiffKeys.fetchKey(tableFailedList);
        Set<String> updateKeySet = fetchUpdateDiffKeys.fetchKey(tableFailedList);
        failed.setTopic(new String[] {tableFailedList.get(0).getTopic()})
            .setStartTime(fetchMinStartTime(tableFailedList))
            .setEndTime(fetchMaxEndTime(tableFailedList))
            .setInsertTotal(fetchTotal(tableFailedList, CheckDiffResult::getInsertTotal))
            .setUpdateTotal(fetchTotal(tableFailedList, CheckDiffResult::getUpdateTotal))
            .setDeleteTotal(fetchTotal(tableFailedList, CheckDiffResult::getDeleteTotal))
            .setKeyInsertSet(getKeyList(insertKeySet, hasMore, "insert key has more;"))
            .setKeyDeleteSet(getKeyList(deleteKeySet, hasMore, "delete key has more;"))
            .setKeyUpdateSet(getKeyList(updateKeySet, hasMore, "update key has more;"));
        long diffSum = tableFailedList.stream().peek(msg -> {
            log.warn("table slice failed info  {} diffSum: {}", msg.getMessage(), msg.getTotalRepair());
        }).mapToLong(CheckDiffResult::getTotalRepair).sum();
        log.warn("result table {} diffSum: {}", resultCommon.getTable(), diffSum);
        failed.setDiffCount(diffSum);
        String message =
            String.format(FAILED_MESSAGE, failed.getInsertTotal(), failed.getUpdateTotal(), failed.getDeleteTotal())
                + resultCommon.getError();
        if (resultCommon.isTableStructureEquals()) {
            failed.setMessage(message);
        }
        failed.setHasMore(hasMore.toString())
            .setRowCount(fetchTotal(tableFailedList, CheckDiffResult::getRowCount) + fetchTotal(successList,
                CheckDiffResult::getRowCount))
            .setCost(calcCheckTaskCost(failed.getStartTime(), failed.getEndTime()));
        return failed;
    }

    private FetchDiffKeys fetchInsertDiffKeys = tableFailedList -> {
        Set<String> diffKey = new TreeSet<>();
        tableFailedList.forEach(tableFailed -> {
            if (CollectionUtils.isNotEmpty(tableFailed.getKeyInsertSet())) {
                diffKey.addAll(tableFailed.getKeyInsertSet());
            }
            if (CollectionUtils.isNotEmpty(tableFailed.getKeyInsert())) {
                diffKey.addAll(tableFailed.getKeyInsert().stream().map(Difference::getKey).toList());
            }
        });
        return diffKey;
    };

    private FetchDiffKeys fetchDeleteDiffKeys = tableFailedList -> {
        Set<String> diffKey = new TreeSet<>();
        tableFailedList.forEach(tableFailed -> {
            if (CollectionUtils.isNotEmpty(tableFailed.getKeyDeleteSet())) {
                diffKey.addAll(tableFailed.getKeyDeleteSet());
            }
            if (CollectionUtils.isNotEmpty(tableFailed.getKeyDelete())) {
                diffKey.addAll(tableFailed.getKeyDelete().stream().map(Difference::getKey).toList());
            }
        });
        return diffKey;
    };

    private FetchDiffKeys fetchUpdateDiffKeys = tableFailedList -> {
        Set<String> diffKey = new TreeSet<>();
        tableFailedList.forEach(tableFailed -> {
            if (CollectionUtils.isNotEmpty(tableFailed.getKeyUpdateSet())) {
                diffKey.addAll(tableFailed.getKeyUpdateSet());
            }
            if (CollectionUtils.isNotEmpty(tableFailed.getKeyUpdate())) {
                diffKey.addAll(tableFailed.getKeyUpdate().stream().map(Difference::getKey).toList());
            }
        });
        return diffKey;
    };

    public void refreshTableStructureDiffResult(CheckTableInfo checkTableInfo) {
        this.checkTableInfo = checkTableInfo;
    }

    @FunctionalInterface
    protected interface FetchDiffKeys {
        /**
         * collect failed keys
         *
         * @param tableFailedList tableFailedList
         * @return keys
         */
        Set<String> fetchKey(List<CheckDiffResult> tableFailedList);
    }

    private long fetchTotal(List<CheckDiffResult> tableFailed,
        java.util.function.ToLongFunction<? super CheckDiffResult> mapper) {
        if (Objects.isNull(tableFailed)) {
            return 0L;
        }
        return tableFailed.stream().mapToLong(mapper).sum();
    }

    private Set<String> getKeyList(Set<String> keySet, StringBuilder hasMore, String message) {
        if (Objects.isNull(keySet) || keySet.size() <= MAX_DISPLAY_SIZE) {
            return keySet;
        }
        hasMore.append(message);
        return Sets.newTreeSet(Iterables.limit(keySet, MAX_DISPLAY_SIZE));
    }

    private CheckSuccess translateCheckSuccess(List<CheckDiffResult> tableSuccessList) {
        CheckSuccess result = new CheckSuccess();
        BeanUtils.copyProperties(tableSuccessList.get(0), result);
        result.setTopic(new String[] {tableSuccessList.get(0).getTopic()});
        result.setStartTime(fetchMinStartTime(tableSuccessList));
        result.setEndTime(fetchMaxEndTime(tableSuccessList));
        result.setRowCount(fetchTotal(tableSuccessList, CheckDiffResult::getRowCount));
        result.setCost(calcCheckTaskCost(result.getStartTime(), result.getEndTime()));
        result.setPartition(tableSuccessList.get(0).getPartitions());
        result.setMessage(fetchMessage(tableSuccessList));
        return result;
    }

    /**
     * Calculation and verification time
     *
     * @param start start
     * @param end end
     * @return Calculation and verification time
     */
    protected long calcCheckTaskCost(LocalDateTime start, LocalDateTime end) {
        if (Objects.nonNull(start) && Objects.nonNull(end)) {
            return Duration.between(start, end).toSeconds();
        }
        return 0;
    }

    private LocalDateTime fetchMaxEndTime(@NotEmpty List<CheckDiffResult> tableResultList) {
        return tableResultList.stream().map(CheckDiffResult::getEndTime).max(LocalDateTime::compareTo).orElse(null);
    }

    private String fetchMessage(@NotEmpty List<CheckDiffResult> tableResultList) {
        List<String> messages =
            tableResultList.stream().map(CheckDiffResult::getMessage).collect(Collectors.toCollection(LinkedList::new));
        if (messages.size() <= MAX_SUCCESS_MESSAGE_SIZE) {
            return messages.toString();
        }
        List<String> limited = new LinkedList<>(messages.subList(0, MAX_SUCCESS_MESSAGE_SIZE));
        limited.add("... and " + (messages.size() - MAX_SUCCESS_MESSAGE_SIZE) + " more slices");
        return limited.toString();
    }

    private LocalDateTime fetchMinStartTime(@NotEmpty List<CheckDiffResult> tableResultList) {
        return tableResultList.stream().map(CheckDiffResult::getStartTime).min(LocalDateTime::compareTo).orElse(null);
    }

    private void reduceFailedRepair(String logFilePath, List<CheckDiffResult> failedList) {
        boolean isCreateRepairSql = ConfigCache.getBooleanValue(ConfigConstants.CREATE_REPAIR_SQL);
        if (isCreateRepairSql) {
            failedList.forEach(tableFailed -> {
                final String repairFile = logFilePath + getRepairFileName(tableFailed);
                repairDeleteDiff(repairFile, tableFailed);
                repairInsertDiff(repairFile, tableFailed);
                repairUpdateDiff(repairFile, tableFailed);
                // immediately Rename Check Failed File
                boolean isImmediately = Objects.equals(tableFailed.getResult(), CheckResultConstants.RESULT_FAILED);
                notifyCsvShardingCompleted(tableFailed, isImmediately);
            });
        } else {
            if (CollectionUtils.isNotEmpty(failedList)) {
                CheckDiffResult tableFailed = failedList.get(0);
                final String repairFile = logFilePath + getRepairFileName(tableFailed);
                appendLogFile(repairFile, List.of());
            }
        }
    }

    private void repairUpdateDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> updateDiffs = tableFailed.getKeyUpdateSet();
        final List<Difference> keyUpdate = tableFailed.getKeyUpdate();
        if (CheckResultUtils.isNotEmptyDiff(updateDiffs, keyUpdate)) {
            RepairEntry update = translateRepairEntry(tableFailed, updateDiffs, keyUpdate);
            update.setType(DML.REPLACE);
            try {
                final List<String> updateRepairs = feignClient.buildRepairStatementUpdateDml(Endpoint.SOURCE, update);
                appendLogFile(repairFile, updateRepairs);
            } catch (Exception ex) {
                log.error("{}build table {} update repair file {}", ErrorCode.BUILD_DIFF_STATEMENT,
                    tableFailed.getTable(), ex.getMessage());
            }
        }
    }

    private RepairEntry translateRepairEntry(CheckDiffResult tableFailed, Set<String> diffsSet,
        List<Difference> keyDifference) {
        RepairEntry repairEntry = new RepairEntry();
        BeanUtils.copyProperties(tableFailed, repairEntry);
        Database sinkDatabase = ConfigCache.getValue(ConfigConstants.DATA_CHECK_SINK_DATABASE, Database.class);
        if (Objects.nonNull(sinkDatabase)) {
            repairEntry.setSchema(sinkDatabase.getSchema());
        }
        repairEntry.setDiffSet(diffsSet).setOgCompatibility(isOgCompatibility).setDiffList(keyDifference);
        return repairEntry;
    }

    private void repairInsertDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> insertDiffs = tableFailed.getKeyInsertSet();
        final List<Difference> keyInsert = tableFailed.getKeyInsert();
        if (CheckResultUtils.isNotEmptyDiff(insertDiffs, keyInsert)) {
            RepairEntry insert = translateRepairEntry(tableFailed, insertDiffs, keyInsert);
            insert.setType(DML.INSERT);
            try {
                final List<String> insertRepairs = feignClient.buildRepairStatementInsertDml(Endpoint.SOURCE, insert);
                appendLogFile(repairFile, insertRepairs);
            } catch (Exception ex) {
                log.error("{}build table {} insert repair file {}", ErrorCode.BUILD_DIFF_STATEMENT,
                    tableFailed.getTable(), ex.getMessage());
            }
        }
    }

    private void repairDeleteDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> deleteDiffs = tableFailed.getKeyDeleteSet();
        List<Difference> keyDelete = tableFailed.getKeyDelete();
        if (CheckResultUtils.isNotEmptyDiff(deleteDiffs, keyDelete)) {
            RepairEntry delete = translateRepairEntry(tableFailed, deleteDiffs, keyDelete);
            delete.setType(DML.DELETE);
            try {
                final List<String> deleteRepairs = feignClient.buildRepairStatementDeleteDml(Endpoint.SOURCE, delete);
                appendLogFile(repairFile, deleteRepairs);
            } catch (Exception ex) {
                log.error("{}build table {} delete repair file {}", ErrorCode.BUILD_DIFF_STATEMENT,
                    tableFailed.getTable(), ex.getMessage());
            }
        }
    }

    private String getRepairFileName(CheckDiffResult tableFailed) {
        final String schema = tableFailed.getSchema();
        final String table = TopicUtil.getTableWithLetter(tableFailed.getTable());
        final int partition = tableFailed.getPartitions();
        return String.format(REPAIR_LOG_TEMPLATE, schema, table, partition);
    }

    private void appendLogFile(String logPath, List<String> resultList) {
        FileUtils.writeAppendFile(logPath, resultList);
    }

    /**
     * Collect all failed check results across tables.
     *
     * @return failed check result list
     */
    public List<CheckDiffResult> getFailedCheckResults() {
        return checkResult.values().stream()
            .flatMap(List::stream)
            .filter(result -> CheckResultConstants.RESULT_FAILED.equals(result.getResult()))
            .collect(Collectors.toList());
    }
}
