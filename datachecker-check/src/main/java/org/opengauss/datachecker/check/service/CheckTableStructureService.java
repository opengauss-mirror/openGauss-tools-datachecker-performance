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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.modules.check.AbstractCheckDiffResultBuilder.CheckDiffResultBuilder;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.report.SliceCheckResultManager;
import org.opengauss.datachecker.check.modules.task.TaskManagerService;
import org.opengauss.datachecker.common.entry.check.CheckTableInfo;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.Database;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * CheckTableStructureService
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/8
 * @since ：11
 */
@Service
public class CheckTableStructureService {
    private static final Logger log = LogUtils.getLogger();
    @Resource
    private CheckEnvironment checkEnvironment;
    @Resource
    private TaskManagerService taskManagerService;
    @Resource
    private EndpointMetaDataManager endpointMetaDataManager;
    @Resource
    private SliceCheckResultManager sliceCheckResultManager;

    private CheckTableInfo checkTableInfo = new CheckTableInfo();

    private final CompareTableStructure tableStructureCompare = (source, sink) -> {
        if (source.size() == sink.size()) {
            final List<String> sourceUpperList = source.stream()
                                                       .map(ColumnsMetaData::getColumnName)
                                                       .map(String::toUpperCase)
                                                       .collect(Collectors.toList());
            final List<String> diffKeyList = sink.stream()
                                                 .map(ColumnsMetaData::getColumnName)
                                                 .map(String::toUpperCase)
                                                 .filter(key -> !sourceUpperList.contains(key))
                                                 .collect(Collectors.toList());
            return diffKeyList.isEmpty();
        } else {
            return false;
        }
    };

    /**
     * Table structure definition field name verification
     *
     * @param processNo
     */
    public CheckTableInfo check(String processNo) {
        initCheckTableStatus();
        checkTableStructureChanged(processNo);
        checkMissTable(processNo);
        log.info("check table structure {}", checkTableInfo);
        sliceCheckResultManager.refreshTableStructureDiffResult(checkTableInfo);
        return checkTableInfo;
    }

    private void initCheckTableStatus() {
        final List<String> checkTableList = endpointMetaDataManager.getCheckTableList();
        final List<String> missTableList = endpointMetaDataManager.getMissTableList();
        List<String> tableList = new LinkedList<>();
        tableList.addAll(checkTableList);
        tableList.addAll(missTableList);
        taskManagerService.initTableExtractStatus(tableList);
        Map<Endpoint, Integer> realTableCount = endpointMetaDataManager.getRealTableCount();
        checkTableInfo.setSinkTableTotalSize(realTableCount.get(Endpoint.SINK));
        checkTableInfo.setSourceTableTotalSize(realTableCount.get(Endpoint.SOURCE));
    }

    private void checkTableStructureChanged(String processNo) {
        final List<String> checkTableList = endpointMetaDataManager.getCheckTableList();
        checkTableList.forEach(tableName -> {
            final TableMetadata sourceMeta = endpointMetaDataManager.getTableMetadata(Endpoint.SOURCE, tableName);
            final TableMetadata sinkMeta = endpointMetaDataManager.getTableMetadata(Endpoint.SINK, tableName);
            checkTableStructureChanged(processNo, tableName, sourceMeta, sinkMeta);
        });
    }

    private void checkMissTable(String processNo) {
        final List<String> missTableList = endpointMetaDataManager.getMissTableList();
        missTableList.forEach(missTable -> {
            final TableMetadata sourceMeta = endpointMetaDataManager.getTableMetadata(Endpoint.SOURCE, missTable);
            checkMissTable(processNo, missTable, sourceMeta);
            if (Objects.equals(checkMissTable(processNo, missTable, sourceMeta), Endpoint.SOURCE)) {
                checkTableInfo.addSource(missTable);
            } else {
                checkTableInfo.addSink(missTable);
            }
        });
        checkTableInfo.setMiss((int) missTableList.stream()
                                                  .distinct()
                                                  .count());
    }

    private void checkTableStructureChanged(String processNo, String tableName, TableMetadata sourceMeta,
        TableMetadata sinkMeta) {
        final boolean isTableStructureEquals = isTableStructureEquals(sourceMeta, sinkMeta);
        if (!isTableStructureEquals) {
            taskManagerService.refreshTableExtractStatus(tableName, Endpoint.CHECK, -1);
            LocalDateTime now = LocalDateTime.now();
            final Database database = checkEnvironment.getDatabase(Endpoint.SOURCE);
            final CheckDiffResultBuilder builder = CheckDiffResultBuilder.builder();
            CheckDiffResult result = builder.process(processNo)
                                            .table(tableName)
                                            .isTableStructureEquals(false)
                                            .startTime(now)
                                            .endTime(now)
                                            .schema(database.getSchema())
                                            .build();
            sliceCheckResultManager.addTableStructureDiffResult(tableName,result);
            log.debug("compared  table[{}] field names not match source={},sink={}", tableName,
                getFieldNames(sourceMeta), getFieldNames(sinkMeta));
        }
    }

    private String getFieldNames(TableMetadata sourceMeta) {
        return sourceMeta.getColumnsMetas()
                         .stream()
                         .map(column -> column.getColumnName() + column.getOrdinalPosition())
                         .collect(Collectors.joining());
    }

    private Endpoint checkMissTable(String processNo, String tableName, TableMetadata sourceMeta) {
        Endpoint onlyExistEndpoint = Objects.isNull(sourceMeta) ? Endpoint.SINK : Endpoint.SOURCE;
        LocalDateTime now = LocalDateTime.now();
        final Database database = checkEnvironment.getDatabase(Endpoint.SOURCE);
        final CheckDiffResultBuilder builder = CheckDiffResultBuilder.builder();
        CheckDiffResult result = builder.process(processNo)
                                        .table(tableName)
                                        .startTime(now)
                                        .endTime(now)
                                        .schema(database.getSchema())
                                        .isExistTableMiss(true, onlyExistEndpoint)
                                        .build();
        taskManagerService.refreshTableExtractStatus(tableName, Endpoint.CHECK, -1);
        sliceCheckResultManager.addTableStructureDiffResult(tableName,result);
        log.error("compared the field names in table[{}](case ignored) and the result is not match", tableName);
        return onlyExistEndpoint;
    }

    private boolean isTableNotExist(TableMetadata sourceMeta, TableMetadata sinkMeta) {
        // one or double endpoint table have not exists, then return false
        return Objects.isNull(sourceMeta) || Objects.isNull(sinkMeta);
    }

    private boolean isTableStructureEquals(TableMetadata sourceMeta, TableMetadata sinkMeta) {
        // one or double endpoint table have not exists, then return false
        if (isTableNotExist(sourceMeta, sinkMeta)) {
            return false;
        }
        return tableStructureCompare.compare(sourceMeta.getPrimaryMetas(), sinkMeta.getPrimaryMetas())
            && tableStructureCompare.compare(sourceMeta.getColumnsMetas(), sinkMeta.getColumnsMetas());
    }

    @FunctionalInterface
    interface CompareTableStructure {
        /**
         * Compare whether the source and destination table structures are the same
         *
         * @param source source
         * @param sink   sink
         * @return Compare Results
         */
        boolean compare(List<ColumnsMetaData> source, List<ColumnsMetaData> sink);
    }
}
