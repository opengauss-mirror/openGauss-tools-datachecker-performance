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

package org.opengauss.datachecker.extract.service;

import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.exception.ProcessMultipleException;
import org.opengauss.datachecker.common.exception.TaskNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DataExtractService
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
public interface DataExtractService {

    /**
     * Extraction task construction
     *
     * @param processNo processNo
     * @return Specify the construction extraction task set under processno
     * @throws ProcessMultipleException The current instance is executing the data extraction service
     *                                  and cannot restart the new verification.
     */
    List<ExtractTask> buildExtractTaskAllTables(String processNo) throws ProcessMultipleException;

    /**
     * Destination task configuration
     *
     * @param processNo processNo
     * @param taskList  taskList
     * @throws ProcessMultipleException The current instance is executing the data extraction service
     *                                  and cannot restart the new verification.
     */
    void buildExtractTaskAllTables(String processNo, List<ExtractTask> taskList) throws ProcessMultipleException;

    /**
     * Execute table data extraction task
     *
     * @param processNo processNo
     * @throws TaskNotFoundException If the task data is empty, an exception TaskNotFoundException will be thrown
     */
    void execExtractTaskAllTables(String processNo) throws TaskNotFoundException;

    /**
     * Clean up the current build task
     */
    void cleanBuildTask();

    /**
     * Query the detailed task information of the specified name under the current process
     *
     * @param taskName taskName
     * @return Task details, if not, return {@code null}
     */
    ExtractTask queryTableInfo(String taskName);

    /**
     * DML statement generating repair report
     *
     * @param schema    schema
     * @param tableName tableName
     * @param diffSet   Primary key set to be generated
     * @return DML statement
     */
    List<String> buildRepairStatementUpdateDml(String schema, String tableName, Set<String> diffSet);

    /**
     * DML statement generating repair report
     *
     * @param schema    schema
     * @param tableName tableName
     * @param diffSet   Primary key set to be generated
     * @return DML statement
     */
    List<String> buildRepairStatementInsertDml(String schema, String tableName, Set<String> diffSet);

    /**
     * DML statement generating repair report
     *
     * @param schema    schema
     * @param tableName tableName
     * @param diffSet   Primary key set to be generated
     * @return DML statement
     */
    List<String> buildRepairStatementDeleteDml(String schema, String tableName, Set<String> diffSet);

    /**
     * Query table data
     *
     * @param tableName       tableName
     * @param compositeKeySet compositeKeySet
     * @return Primary key corresponds to table data
     */
    List<Map<String, String>> queryTableColumnValues(String tableName, List<String> compositeKeySet);

    /**
     * Query the metadata information of the current table structure and hash
     *
     * @param tableName tableName
     * @return Table structure hash
     */
    TableMetadataHash queryTableMetadataHash(String tableName);

    /**
     * PK list data is specified in the query table, and hash is used for secondary verification data query
     *
     * @param dataLog dataLog
     * @return row data hash
     */
    List<RowDataHash> querySecondaryCheckRowData(SourceDataLog dataLog);

    /**
     * Get the current endpoint configuration information
     *
     * @return ExtractConfig
     */
    ExtractConfig getEndpointConfig();

    /**
     * queryIncrementMetaData
     *
     * @param tableName tableName
     * @return TableMetadata
     */
    TableMetadata queryIncrementMetaData(String tableName);

    /**
     * check current database check table-count is zero
     *
     * @param isForced true is forced to query database
     * @return result
     */
    Boolean isCheckTableEmpty(boolean isForced);
}
