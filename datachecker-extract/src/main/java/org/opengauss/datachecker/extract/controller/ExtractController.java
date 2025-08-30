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

package org.opengauss.datachecker.extract.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotEmpty;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.PageExtract;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.exception.ProcessMultipleException;
import org.opengauss.datachecker.common.exception.TaskNotFoundException;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.service.DataExtractService;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * data extracton service
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Tag(name = "data extracton service")
@RestController
public class ExtractController {
    @Resource
    private MetaDataService metaDataService;
    @Resource
    private DataExtractService dataExtractService;

    /**
     * loading database metadata information
     * including the table name,primary key field information list, and column field information list
     *
     * @return database metadata information
     */
    @PostMapping("/extract/load/page/meta/data")
    public Result<Map<String, TableMetadata>> queryMetaDataOfSchema(@RequestBody PageExtract pageExtract) {
        Map<String, TableMetadata> metaDataMap = metaDataService.queryMetaDataOfSchemaCache(pageExtract);
        return Result.success(metaDataMap);
    }

    /**
     * 获取元数据信息分页提取对象
     *
     * @return PageExtractTask
     */
    @GetMapping("/get/extract/meta/page/info")
    public Result<PageExtract> getExtractMetaPageInfo() {
        return Result.success(metaDataService.getExtractMetaPageInfo());
    }

    /**
     * source endpoint extraction task construction
     *
     * @param processNo execution process no
     * @throws ProcessMultipleException the data extraction service is being executed for the current instance.
     *                                  new verification process cannot be enabled.
     */
    @Operation(summary = "construction a data extraction task for the current endpoint")
    @PostMapping("/extract/build/task/all")
    public Result<PageExtract> buildExtractTaskAllTables(
            @Parameter(name = "processNo", description = "execution process no") @RequestParam(name = "processNo")
            String processNo) {
        return Result.success(dataExtractService.buildExtractTaskAllTables(processNo));
    }

    /**
     * 分页获取抽取任务数据
     *
     * @param pageExtract pageExtract
     * @return 任务列表
     */
    @PostMapping("/extract/build/task/page")
    public Result<List<ExtractTask>> fetchExtractTaskPageTables(@RequestBody PageExtract pageExtract) {
        return Result.success(dataExtractService.fetchExtractTaskPageTables(pageExtract));
    }

    /**
     * sink endpoint task configuration
     *
     * @param taskList task list
     * @throws ProcessMultipleException the data extraction service is being executed for the current instance.
     *                                  new verification process cannot be enabled.
     */
    @Operation(summary = "sink endpoint task configuration")
    @PostMapping("/dispatch/sink/extract/task/page")
    Result<Void> dispatchSinkExtractTaskPage(@RequestBody List<ExtractTask> taskList) {
        dataExtractService.dispatchSinkExtractTaskPage(taskList);
        return Result.success();
    }

    /**
     * full extraction service processing flow:
     * 1、create task information based on the table name
     * 2、build task thread based on task information
     * 2.1 thread pool configuration
     * 2.2 task thread construction
     * 3、extract data
     * 3.1 JDBC extraction, data processing, and data hash calculation
     * 3.2 data encapsulation ,pushing kafka
     * <p>
     * execution a table data extraction task
     *
     * @param processNo execution process no
     * @throws TaskNotFoundException if the task data is empty,the TaskNotFoundException exception is thrown.
     */
    @Operation(summary = "execute the data extraction task that has been created for the current endpoint")
    @PostMapping("/extract/exec/task/all")
    public Result<Void> execExtractTaskAllTables(
            @Parameter(name = "processNo", description = "execution process no") @RequestParam(name = "processNo")
            String processNo) {
        dataExtractService.execExtractTaskAllTables(processNo);
        return Result.success();
    }

    /**
     * clear the cached task information of the corresponding endpoint and rest the task.
     *
     * @return interface invoking result
     */
    @Operation(summary = " clear the cached task information of the corresponding endpoint and rest the task.")
    @PostMapping("/extract/clean/build/task")
    public Result<Void> cleanBuildedTask() {
        dataExtractService.cleanBuildTask();
        return Result.success();
    }

    /**
     * queries information about data extraction tasks in a specified table in the current process.
     *
     * @param tableName table name
     * @return table data extraction task information
     */
    @GetMapping("/extract/table/info")
    Result<ExtractTask> queryTableInfo(@RequestParam(name = "tableName") String tableName) {
        return Result.success(dataExtractService.queryTableInfo(tableName));
    }

    /**
     * querying table data
     *
     * @param tableName       table name
     * @param compositeKeySet primary key set
     * @return table record data
     */
    @Operation(summary = "querying table data")
    @PostMapping("/extract/query/table/data")
    Result<List<Map<String, String>>> queryTableColumnValues(
            @NotEmpty(message = "the name of the table to be repaired belongs cannot be empty")
            @RequestParam(name = "tableName") String tableName,
            @NotEmpty(message = "the primary key set to be repaired belongs cannot be empty") @RequestBody
            Set<String> compositeKeySet) {
        return Result.success(dataExtractService.queryTableColumnValues(tableName, new ArrayList<>(compositeKeySet)));
    }

    /**
     * Query the hash value of the metadata of the table
     *
     * @param tableName tableName
     * @return TableMetadataHash
     */
    @PostMapping("/extract/source/table/metadata/hash")
    Result<TableMetadataHash> querySourceTableMetadataHash(@RequestParam(name = "tableName") String tableName) {
        return Result.success(dataExtractService.queryTableMetadataHash(tableName));
    }

    /**
     * Query the hash value of the metadata of the table
     *
     * @param tableName tableName
     * @return TableMetadataHash
     */
    @PostMapping("/extract/sink/table/metadata/hash")
    Result<TableMetadataHash> querySinkTableMetadataHash(@RequestParam(name = "tableName") String tableName) {
        return Result.success(dataExtractService.queryTableMetadataHash(tableName));
    }

    /**
     * queryIncrementMetaData
     *
     * @param tableName tableName
     * @return TableMetadata
     */
    @GetMapping("/extract/query/increment/metadata")
    Result<TableMetadata> queryIncrementMetaData(@RequestParam(name = "tableName") String tableName) {
        return Result.success(dataExtractService.queryIncrementMetaData(tableName));
    }

    /**
     * Query the hash value of the row data of the table;
     * query is async, and the result is returned query id
     *
     * @param dataLog dataLog
     * @return query id
     */
    @PostMapping("/extract/data/row/hash/async")
    Result<String> queryCheckRowDataAsync(@RequestBody SourceDataLog dataLog) {
        return Result.success(dataExtractService.querySecondaryCheckRowDataAsync(dataLog));
    }

    /**
     * query check row data async status
     *
     * @param queryId query id
     * @return query status
     */
    @PostMapping("/extract/data/row/hash/async/status")
    Result<Boolean> queryCheckRowDataAsyncStatus(@RequestParam(name = "queryId") String queryId) {
        return Result.success(dataExtractService.querySecondaryCheckRowDataStatusByQueryId(queryId));
    }

    /**
     * query check row data async data
     *
     * @param queryId query id
     * @return row data list
     */
    @PostMapping("/extract/data/row/hash/async/data")
    Result<List<RowDataHash>> queryCheckRowDataAsyncData(@RequestParam(name = "queryId") String queryId) {
        return Result.success(dataExtractService.querySecondaryCheckRowDataByQueryId(queryId));
    }

    /**
     * Get the current endpoint configuration information
     *
     * @return ExtractConfig
     */
    @GetMapping("/extract/config")
    Result<ExtractConfig> getEndpointConfig() {
        return Result.success(dataExtractService.getEndpointConfig());
    }

    /**
     * check schema whether has tables; if isForced is true,query by jdbc, else queried in cache
     *
     * @param isForced isForced
     * @return has tables
     */
    @GetMapping("/check/table/empty")
    Result<Boolean> isCheckTableEmpty(@RequestParam(name = "isForced") boolean isForced) {
        return Result.success(metaDataService.mdsIsCheckTableEmpty(isForced));
    }
}
