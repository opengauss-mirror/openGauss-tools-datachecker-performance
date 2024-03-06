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

import javax.annotation.Resource;
import javax.validation.constraints.NotEmpty;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final int MAX_DISPLAY_SIZE = 200;

    private final Map<String, Integer> tableSliceCountMap = new ConcurrentHashMap<>();
    private final Map<String, List<CheckDiffResult>> checkResult = new ConcurrentHashMap<>();
    private final Map<String, CheckDiffResult> tableStructureResult = new ConcurrentHashMap<>();

    @Resource
    private FeignClientService feignClient;
    private int successTableCount = 0;
    private int failedTableCount = 0;
    private int rowCount = 0;
    private boolean isCsvMode = false;
    private boolean ogCompatibility = false;
    private boolean hasInitSliceResultEnvironment = true;
    private CheckTableInfo checkTableInfo = null;

    /**
     * add slice check result
     *
     * @param slice  slice
     * @param result result
     */
    public void addResult(SliceVo slice, CheckDiffResult result) {
        if (!tableSliceCountMap.containsKey(slice.getTable())) {
            tableSliceCountMap.put(slice.getTable(), slice.getTotal());
        }
        if (hasInitSliceResultEnvironment) {
            isCsvMode = Objects.equals(ConfigCache.getCheckMode(), CheckMode.CSV);
            ogCompatibility = feignClient.checkTargetOgCompatibility();
            hasInitSliceResultEnvironment = false;
            LogUtils.info(log, "my gold ,i want to set property hasInitSliceResultEnvironment, and execute once");
        }
        addResult(slice.getTable(), result);
    }

    public void addTableStructureDiffResult(SliceVo slice, CheckDiffResult result) {
        addTableStructureDiffResult(slice.getTable(), result);
    }

    public void addTableStructureDiffResult(String table, CheckDiffResult result) {
        if (tableStructureResult.containsKey(table)) {
            return;
        }
        tableStructureResult.put(table, result);
        failedTableCount++;
        CheckFailed failed = translateCheckFailed(List.of(result));
        String failedLogPath = ConfigCache.getCheckResult() + CheckResultConstants.FAILED_LOG_NAME;
        FileUtils.writeAppendFile(failedLogPath, JsonObjectUtil.prettyFormatMillis(failed) + ",");
        refreshSummary();
    }

    /**
     * add table check result
     *
     * @param table           table name
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
        boolean immediatelyRenameCheckSuccessFile =
            Objects.equals(checkDiffResult.getResult(), CheckResultConstants.RESULT_SUCCESS);
        notifyCsvShardingCompleted(checkDiffResult, immediatelyRenameCheckSuccessFile);
        if (completedSliceSize.get() == tableSliceCountMap.get(table)) {
            List<CheckDiffResult> results = checkResult.get(table);
            Map<String, List<CheckDiffResult>> resultMap;
            resultMap = results.stream()
                               .collect(Collectors.groupingBy(CheckDiffResult::getResult));
            String checkResultPath = ConfigCache.getCheckResult();
            if (resultMap.containsKey(CheckResultConstants.RESULT_FAILED)) {
                List<CheckDiffResult> tableFiledList = resultMap.get(CheckResultConstants.RESULT_FAILED);
                CheckFailed failed = translateCheckFailed(tableFiledList);
                String failedLogPath = checkResultPath + CheckResultConstants.FAILED_LOG_NAME;
                FileUtils.writeAppendFile(failedLogPath, JsonObjectUtil.prettyFormatMillis(failed) + ",");
                reduceFailedRepair(checkResultPath, tableFiledList);
                if (isCsvMode) {
                    List<CheckCsvFailed> csvFailedList = translateCheckCsvFaileds(results);
                    String csvFailedLogPath = checkResultPath + CheckResultConstants.CSV_FAILED_DETAIL_NAME;
                    saveCsvSliceFailedDetails(csvFailedList, csvFailedLogPath);
                }
                failedTableCount++;
            } else {
                List<CheckDiffResult> tableSuccessList = resultMap.get(CheckResultConstants.RESULT_SUCCESS);
                CheckSuccess success = translateCheckSuccess(tableSuccessList);
                String successLogPath = checkResultPath + SUCCESS_LOG_NAME;
                FileUtils.writeAppendFile(successLogPath, JsonObjectUtil.prettyFormatMillis(success) + ",");
                successTableCount++;
                rowCount += success.getRowCount();
            }
            refreshSummary();
        }
    }

    private void saveCsvSliceFailedDetails(List<CheckCsvFailed> csvFailedList, String csvFailedLogPath) {
        if (CollectionUtils.isEmpty(csvFailedList)) {
            return;
        }
        csvFailedList.stream()
                     .filter(CheckCsvFailed::isNotEmpty)
                     .forEach(csvFailed -> {
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
                                                         .build(result.getFileName(), result.getKeyInsert(),
                                                             result.getKeyUpdate(), result.getKeyDelete()))
                      .collect(Collectors.toList());
    }

    /**
     * refresh summary log
     */
    public void refreshSummary() {
        CheckSummary checkSummary = new CheckSummary();
        int completeCount = successTableCount + failedTableCount;
        checkSummary.setMode(ConfigCache.getCheckMode());
        checkSummary.setTableCount(completeCount);
        checkSummary.setMissTable(checkTableInfo);
        checkSummary.setStartTime(ConfigCache.getValue(ConfigConstants.START_LOCAL_TIME, LocalDateTime.class));
        checkSummary.setEndTime(LocalDateTime.now());
        checkSummary.setCost(calcCheckTaskCost(checkSummary.getStartTime(), checkSummary.getEndTime()));
        checkSummary.setSuccessCount(successTableCount);
        checkSummary.setFailedCount(failedTableCount);
        checkSummary.setRowCount(rowCount);
        String summaryPath = ConfigCache.getCheckResult() + SUMMARY_LOG_NAME;
        FileUtils.writeFile(summaryPath, JsonObjectUtil.prettyFormatMillis(checkSummary));
    }

    private CheckFailed translateCheckFailed(List<CheckDiffResult> tableFiledList) {
        CheckFailed failed = new CheckFailed();
        CheckDiffResult resultCommon = tableFiledList.get(0);
        BeanUtils.copyProperties(resultCommon, failed);
        StringBuilder hasMore = new StringBuilder();
        Set<String> insertKeySet = fetchInsertDiffKeys.fetchKey(tableFiledList);
        Set<String> deleteKeySet = fetchDeleteDiffKeys.fetchKey(tableFiledList);
        Set<String> updateKeySet = fetchUpdateDiffKeys.fetchKey(tableFiledList);
        failed.setTopic(new String[] {tableFiledList.get(0).getTopic()})
              .setDiffCount(fetchTotalRepair(tableFiledList))
              .setStartTime(fetchMinStartTime(tableFiledList))
              .setEndTime(fetchMaxEndTime(tableFiledList))
              .setKeyInsertSet(getKeyList(insertKeySet, hasMore, "insert key has more;"))
              .setKeyDeleteSet(getKeyList(deleteKeySet, hasMore, "delete key has more;"))
              .setKeyUpdateSet(getKeyList(updateKeySet, hasMore, "update key has more;"));
        String message = String.format(FAILED_MESSAGE, failed.getKeyInsertSize(), failed.getKeyUpdateSize(),
            failed.getKeyDeleteSize());
        if (resultCommon.isTableStructureEquals()) {
            failed.setMessage(message);
        }
        failed.setHasMore(hasMore.toString())
              .setRowCount(fetchRowCount.fetchCount(tableFiledList))
              .setCost(calcCheckTaskCost(failed.getStartTime(), failed.getEndTime()));
        return failed;
    }

    private FetchRowCount fetchRowCount = list -> list.stream()
                                                      .mapToLong(CheckDiffResult::getRowCount)
                                                      .sum();

    private FetchDiffKeys fetchInsertDiffKeys = tableFiledList -> {
        Set<String> diffKey = new TreeSet<>();
        tableFiledList.forEach(tableFiled -> {
            if (CollectionUtils.isNotEmpty(tableFiled.getKeyInsertSet())) {
                diffKey.addAll(tableFiled.getKeyInsertSet());
            }
            if (CollectionUtils.isNotEmpty(tableFiled.getKeyInsert())) {
                diffKey.addAll(tableFiled.getKeyInsert()
                                         .stream()
                                         .map(Difference::getKey)
                                         .collect(Collectors.toList()));
            }
        });
        return diffKey;
    };
    private FetchDiffKeys fetchDeleteDiffKeys = tableFiledList -> {
        Set<String> diffKey = new TreeSet<>();
        tableFiledList.forEach(tableFiled -> {
            if (CollectionUtils.isNotEmpty(tableFiled.getKeyDeleteSet())) {
                diffKey.addAll(tableFiled.getKeyDeleteSet());
            }
            if (CollectionUtils.isNotEmpty(tableFiled.getKeyDelete())) {
                diffKey.addAll(tableFiled.getKeyDelete()
                                         .stream()
                                         .map(Difference::getKey)
                                         .collect(Collectors.toList()));
            }
        });
        return diffKey;
    };
    private FetchDiffKeys fetchUpdateDiffKeys = tableFiledList -> {
        Set<String> diffKey = new TreeSet<>();
        tableFiledList.forEach(tableFiled -> {
            if (CollectionUtils.isNotEmpty(tableFiled.getKeyUpdateSet())) {
                diffKey.addAll(tableFiled.getKeyUpdateSet());
            }
            if (CollectionUtils.isNotEmpty(tableFiled.getKeyUpdate())) {
                diffKey.addAll(tableFiled.getKeyUpdate()
                                         .stream()
                                         .map(Difference::getKey)
                                         .collect(Collectors.toList()));
            }
        });
        return diffKey;
    };

    public void refreshTableStructureDiffResult(CheckTableInfo checkTableInfo) {
        this.checkTableInfo = checkTableInfo;
    }

    @FunctionalInterface
    protected interface FetchDiffKeys {
        Set<String> fetchKey(List<CheckDiffResult> tableFiledList);
    }

    @FunctionalInterface
    protected interface FetchRowCount {
        long fetchCount(List<CheckDiffResult> tableFiledList);
    }

    private long fetchTotalRepair(List<CheckDiffResult> tableFiled) {
        return tableFiled.stream()
                         .mapToLong(CheckDiffResult::getTotalRepair)
                         .sum();
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
        result.setRowCount(fetchRowCount.fetchCount(tableSuccessList));
        result.setCost(calcCheckTaskCost(result.getStartTime(), result.getEndTime()));
        result.setPartition(tableSuccessList.get(0)
                                            .getPartitions());
        result.setMessage(fetchMessage(tableSuccessList));
        return result;
    }

    /**
     * Calculation and verification time
     *
     * @param start start
     * @param end   end
     * @return
     */
    protected long calcCheckTaskCost(LocalDateTime start, LocalDateTime end) {
        if (Objects.nonNull(start) && Objects.nonNull(end)) {
            return Duration.between(start, end)
                           .toSeconds();
        }
        return 0;
    }

    private LocalDateTime fetchMaxEndTime(@NotEmpty List<CheckDiffResult> tableResultList) {
        return tableResultList.stream()
                              .map(CheckDiffResult::getEndTime)
                              .max(LocalDateTime::compareTo)
                              .get();
    }

    private String fetchMessage(@NotEmpty List<CheckDiffResult> tableResultList) {
        return tableResultList.stream()
                              .map(CheckDiffResult::getMessage)
                              .collect(Collectors.toList())
                              .toString();
    }

    private LocalDateTime fetchMinStartTime(@NotEmpty List<CheckDiffResult> tableResultList) {
        return tableResultList.stream()
                              .map(CheckDiffResult::getStartTime)
                              .min(LocalDateTime::compareTo)
                              .get();
    }

    private void reduceFailedRepair(String logFilePath, List<CheckDiffResult> failedList) {
        failedList.forEach(tableFailed -> {
            final String repairFile = logFilePath + getRepairFileName(tableFailed);
            repairDeleteDiff(repairFile, tableFailed);
            repairInsertDiff(repairFile, tableFailed);
            repairUpdateDiff(repairFile, tableFailed);
            boolean immediatelyRenameCheckFailedFile =
                Objects.equals(tableFailed.getResult(), CheckResultConstants.RESULT_FAILED);
            notifyCsvShardingCompleted(tableFailed, immediatelyRenameCheckFailedFile);
        });
    }

    private void repairUpdateDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> updateDiffs = tableFailed.getKeyUpdateSet();
        final List<Difference> keyUpdate = tableFailed.getKeyUpdate();
        if (CheckResultUtils.isNotEmptyDiff(updateDiffs, keyUpdate)) {
            RepairEntry update = translateRepairEntry(tableFailed, updateDiffs, keyUpdate);
            update.setType(DML.REPLACE);
            final List<String> updateRepairs = feignClient.buildRepairStatementUpdateDml(Endpoint.SOURCE, update);
            appendLogFile(repairFile, updateRepairs);
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
        repairEntry.setDiffSet(diffsSet)
                   .setOgCompatibility(ogCompatibility)
                   .setDiffList(keyDifference);
        return repairEntry;
    }

    private void repairInsertDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> insertDiffs = tableFailed.getKeyInsertSet();
        final List<Difference> keyInsert = tableFailed.getKeyInsert();
        if (CheckResultUtils.isNotEmptyDiff(insertDiffs, keyInsert)) {
            RepairEntry insert = translateRepairEntry(tableFailed, insertDiffs, keyInsert);
            insert.setType(DML.INSERT);
            final List<String> insertRepairs = feignClient.buildRepairStatementInsertDml(Endpoint.SOURCE, insert);
            appendLogFile(repairFile, insertRepairs);
        }
    }

    private void repairDeleteDiff(String repairFile, CheckDiffResult tableFailed) {
        final Set<String> deleteDiffs = tableFailed.getKeyDeleteSet();
        List<Difference> keyDelete = tableFailed.getKeyDelete();
        if (CheckResultUtils.isNotEmptyDiff(deleteDiffs, keyDelete)) {
            RepairEntry delete = translateRepairEntry(tableFailed, deleteDiffs, keyDelete);
            delete.setType(DML.DELETE);
            final List<String> deleteRepairs = feignClient.buildRepairStatementDeleteDml(Endpoint.SOURCE, delete);
            appendLogFile(repairFile, deleteRepairs);
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
}
