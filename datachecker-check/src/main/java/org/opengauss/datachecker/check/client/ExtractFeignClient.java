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

package org.opengauss.datachecker.check.client;

import org.opengauss.datachecker.common.entry.common.GlobalConfig;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.csv.CsvPathConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
public interface ExtractFeignClient {
    /**
     * Service health check
     *
     * @return Return the corresponding result of the interface
     */
    @GetMapping("/extract/health")
    Result<Void> health();

    /**
     * Endpoint loading metadata information
     *
     * @return Return metadata
     */
    @GetMapping("/extract/load/database/meta/data")
    Result<Map<String, TableMetadata>> queryMetaDataOfSchema();

    /**
     * Extraction task construction
     *
     * @param processNo Execution process number
     * @return Return to build task collection
     */
    @PostMapping("/extract/build/task/all")
    Result<List<ExtractTask>> buildExtractTaskAllTables(@RequestParam(name = "processNo") String processNo);

    /**
     * Destination extraction task configuration
     *
     * @param processNo Execution process number
     * @param taskList  Source side task list
     * @return Request results
     */
    @PostMapping("/extract/config/sink/task/all")
    Result<Void> buildExtractTaskAllTables(@RequestParam(name = "processNo") String processNo,
        @RequestBody List<ExtractTask> taskList);

    /**
     * Full extraction business processing flow
     *
     * @param processNo Execution process sequence number
     * @return Request results
     */
    @PostMapping("/extract/exec/task/all")
    Result<Void> execExtractTaskAllTables(@RequestParam(name = "processNo") String processNo);

    /**
     * Query the specified topic data
     *
     * @param tableName  table Name
     * @param partitions topic partitions
     * @return topic data
     */
    @GetMapping("/extract/query/topic/data")
    Result<List<RowDataHash>> queryTopicData(@RequestParam("tableName") String tableName,
        @RequestParam("partitions") int partitions);

    /**
     * Query the specified incremental topic data
     *
     * @param tableName table Name
     * @return topic data
     */
    @GetMapping("/extract/query/increment/topic/data")
    Result<List<RowDataHash>> queryIncrementTopicData(@RequestParam("tableName") String tableName);

    /**
     * Clean up the opposite environment
     *
     * @param processNo Execution process sequence number
     * @return Request results
     */
    @PostMapping("/extract/clean/environment")
    Result<Void> cleanEnvironment(@RequestParam(name = "processNo") String processNo);

    /**
     * Clear the extraction end task cache
     *
     * @return Request results
     */
    @PostMapping("/extract/clean/task")
    Result<Void> cleanTask();

    /**
     * Build repair statements based on parameters
     *
     * @param repairEntry repairEntry
     * @return Return to repair statement collection
     */
    @PostMapping("/extract/build/repair/statement/update")
    Result<List<String>> buildRepairStatementUpdateDml(@RequestBody RepairEntry repairEntry);

    /**
     * Build repair statements based on parameters
     *
     * @param repairEntry repairEntry
     * @return Return to repair statement collection
     */
    @PostMapping("/extract/build/repair/statement/insert")
    Result<List<String>> buildRepairStatementInsertDml(@RequestBody RepairEntry repairEntry);

    /**
     * Build repair statements based on parameters
     *
     * @param repairEntry repairEntry
     * @return Return to repair statement collection
     */
    @PostMapping("/extract/build/repair/statement/delete")
    Result<List<String>> buildRepairStatementDeleteDml(@RequestBody RepairEntry repairEntry);

    /**
     * Query table metadata hash information
     *
     * @param tableName tableName
     * @return Table metadata hash
     */
    @PostMapping("/extract/query/table/metadata/hash")
    Result<TableMetadataHash> queryTableMetadataHash(@RequestParam(name = "tableName") String tableName);

    /**
     * Extract incremental log data records
     *
     * @param dataLog data Log
     * @return Return extraction results
     */
    @PostMapping("/extract/query/secondary/data/row/hash")
    Result<List<RowDataHash>> querySecondaryCheckRowData(@RequestBody SourceDataLog dataLog);

    /**
     * Get the current endpoint configuration information
     *
     * @return ExtractConfig
     */
    @GetMapping("/extract/config")
    Result<ExtractConfig> getEndpointConfig();

    /**
     * start source increment monitor
     *
     * @return void
     */
    @PostMapping("/start/source/increment/monitor")
    Result<Void> startIncrementMonitor();

    /**
     * pause or resume increment monitor
     *
     * @param parseOrResume
     * @return void
     */
    @PostMapping("/pause/resume/increment/monitor")
    Result<Void> pauseOrResumeIncrementMonitor(@RequestParam("parseOrResume") boolean parseOrResume);

    /**
     * Distribution Data Extraction config
     *
     * @param config config
     * @return void
     */
    @PostMapping("/extract/config/distribute")
    Result<Void> distributeConfig(@RequestBody GlobalConfig config);

    @PostMapping("/csv/config/distribute")
    Result<Void> distributeConfig(@RequestBody CsvPathConfig config);

    @PostMapping("/extract/shutdown")
    Result<Void> shutdown(@RequestBody String message);

    /**
     * queryIncrementMetaData
     *
     * @param tableName tableName
     * @return TableMetadata
     */
    @GetMapping("/extract/query/increment/metadata")
    Result<TableMetadata> queryIncrementMetaData(@RequestParam(name = "tableName") String tableName);

    @PostMapping("/notify/check/finished")
    Result<Void> notifyCheckTableFinished(@RequestParam(name = "tableName") String tableName);

    @GetMapping("/check/table/empty")
    Result<Boolean> isCheckTableEmpty(@RequestParam(name = "isForced") boolean isForced);

    @GetMapping("/check/target/og/compatibility")
    Result<Boolean> checkTargetOgCompatibility();

    @PostMapping("/start/csv/service")
    Result<Void> enableCsvExtractService();

    @GetMapping("/fetch/check/table/count")
    Result<Integer> fetchCheckTableCount();
}
