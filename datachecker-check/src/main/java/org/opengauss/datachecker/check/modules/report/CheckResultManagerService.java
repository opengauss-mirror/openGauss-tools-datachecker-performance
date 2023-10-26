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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.event.CheckFailedReportEvent;
import org.opengauss.datachecker.check.event.CheckSuccessReportEvent;
import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.CheckResultConstants;
import org.opengauss.datachecker.common.entry.check.CheckPartition;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.report.CheckProgress;
import org.opengauss.datachecker.common.entry.report.CheckSummary;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/24
 * @since ：11
 */
@Service
public class CheckResultManagerService implements ApplicationContextAware {
    private static final Logger log = LogUtils.getLogger();
    private static final String SUMMARY_LOG_NAME = "summary.log";
    private static final String REPAIR_LOG_TEMPLATE = "repair_%s_%s_%s.txt";

    private ApplicationContext context;
    @Resource
    private ProgressService progressService;
    @Resource
    private SliceProgressService sliceProgressService;
    @Resource
    private CheckEnvironment checkEnvironment;
    @Resource
    private FeignClientService feignClient;

    private final Map<CheckPartition, CheckDiffResult> checkResultCache = new ConcurrentHashMap<>();
    private final Map<String, CheckDiffResult> noCheckedCache = new ConcurrentHashMap<>();

    /**
     * Add Merkel verification result
     *
     * @param checkPartition  checkPartition
     * @param checkDiffResult checkDiffResult
     */
    public void addResult(CheckPartition checkPartition, CheckDiffResult checkDiffResult) {
        checkResultCache.put(checkPartition, checkDiffResult);
        if (StringUtils.equals(CheckResultConstants.RESULT_SUCCESS, checkDiffResult.getResult())) {
            context.publishEvent(new CheckSuccessReportEvent(checkDiffResult));
        } else {
            context.publishEvent(new CheckFailedReportEvent(checkDiffResult));
        }
    }

    /**
     * Summary of verification results
     *
     * @param tableName       tableName
     * @param checkDiffResult checkDiffResult
     */
    public void addNoCheckedResult(String tableName, CheckDiffResult checkDiffResult) {
        noCheckedCache.put(tableName, checkDiffResult);
        context.publishEvent(new CheckFailedReportEvent(checkDiffResult));
    }

    /**
     * Summary of verification results
     */
    public void summaryCheckResult() {
        try {
            String logFilePath = getLogRootPath();
            final List<CheckDiffResult> successList = filterResultByResult(CheckResultConstants.RESULT_SUCCESS);
            final List<CheckDiffResult> failedList = filterResultByResult(CheckResultConstants.RESULT_FAILED);
            boolean ogCompatibility = feignClient.checkTargetOgCompatibility();
            reduceFailedRepair(logFilePath, failedList, ogCompatibility);
            reduceSummary(successList, failedList);
        } catch (Exception exception) {
            log.error("summaryCheckResult ", exception);
        } finally {
            checkResultCache.clear();
            noCheckedCache.clear();
        }
    }

    private void reduceFailedRepair(String logFilePath, List<CheckDiffResult> failedList, boolean ogCompatibility) {
        failedList.forEach(tableFailed -> {
            final String repairFile = logFilePath + getRepairFileName(tableFailed);
            repairDeleteDiff(repairFile, tableFailed, ogCompatibility);
            repairInsertDiff(repairFile, tableFailed, ogCompatibility);
            repairUpdateDiff(repairFile, tableFailed, ogCompatibility);
        });
    }

    private void repairUpdateDiff(String repairFile, CheckDiffResult tableFailed, boolean ogCompatibility) {
        final Set<String> updateDiffs = tableFailed.getKeyUpdateSet();
        if (CollectionUtils.isNotEmpty(updateDiffs)) {
            RepairEntry update = new RepairEntry();
            update.setTable(tableFailed.getTable()).setSchema(tableFailed.getSchema())
                  .setOgCompatibility(ogCompatibility).setDiffSet(updateDiffs);
            final List<String> updateRepairs = feignClient.buildRepairStatementUpdateDml(Endpoint.SOURCE, update);
            appendLogFile(repairFile, updateRepairs);
        }
    }

    private void repairInsertDiff(String repairFile, CheckDiffResult tableFailed, boolean ogCompatibility) {
        final Set<String> insertDiffs = tableFailed.getKeyInsertSet();
        if (CollectionUtils.isNotEmpty(insertDiffs)) {
            RepairEntry insert = new RepairEntry();
            insert.setTable(tableFailed.getTable()).setSchema(tableFailed.getSchema())
                  .setOgCompatibility(ogCompatibility).setDiffSet(insertDiffs);
            final List<String> insertRepairs = feignClient.buildRepairStatementInsertDml(Endpoint.SOURCE, insert);
            appendLogFile(repairFile, insertRepairs);
        }
    }

    private void repairDeleteDiff(String repairFile, CheckDiffResult tableFailed, boolean ogCompatibility) {
        final Set<String> deleteDiffs = tableFailed.getKeyDeleteSet();
        if (CollectionUtils.isNotEmpty(deleteDiffs)) {
            RepairEntry delete = new RepairEntry();
            delete.setTable(tableFailed.getTable()).setSchema(tableFailed.getSchema())
                  .setOgCompatibility(ogCompatibility).setDiffSet(deleteDiffs);
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

    private String getLogRootPath() {
        final String exportCheckPath = checkEnvironment.getExportCheckPath();
        return exportCheckPath + File.separatorChar + "result" + File.separatorChar;
    }

    private void reduceSummary(List<CheckDiffResult> successList, List<CheckDiffResult> failedList) {
        String logFilePath = getLogRootPath();
        int successTableCount = calcTableCount(successList);
        int failedTableCount = calcTableCount(failedList);
        CheckSummary checkSummary = buildCheckSummaryResult(successTableCount, failedTableCount);
        String summaryPath = logFilePath + SUMMARY_LOG_NAME;
        FileUtils.writeFile(summaryPath, JsonObjectUtil.prettyFormatMillis(checkSummary));
    }

    private CheckSummary buildCheckSummaryResult(int successTableCount, int failedTableCount) {
        CheckSummary checkSummary = new CheckSummary();
        int completeCount = successTableCount + failedTableCount;
        final CheckProgress checkProgress = sliceProgressService.getCheckProgress();
        checkSummary.setMode(checkEnvironment.getCheckMode());
        checkSummary.setTableCount(completeCount);
        checkSummary.setStartTime(checkProgress.getStartTime());
        checkSummary.setEndTime(checkProgress.getEndTime());
        checkSummary.setCost(checkProgress.getCost());
        checkSummary.setSuccessCount(successTableCount);
        checkSummary.setFailedCount(failedTableCount);
        return checkSummary;
    }

    private List<CheckDiffResult> filterResultByResult(String resultType) {
        List<CheckDiffResult> resultList = new LinkedList<>(
            checkResultCache.values().stream().filter(result -> result.getResult().equals(resultType))
                            .collect(Collectors.toList()));
        if (CheckResultConstants.RESULT_FAILED.equals(resultType)) {
            resultList.addAll(noCheckedCache.values());
        }
        return resultList;
    }

    private int calcTableCount(List<CheckDiffResult> resultList) {
        return (int) resultList.stream().map(CheckDiffResult::getTable).distinct().count();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    public void progressing(int tableCount) {
        progressService.resetProgress(tableCount);
    }
}
