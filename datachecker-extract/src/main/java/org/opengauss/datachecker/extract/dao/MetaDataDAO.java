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

package org.opengauss.datachecker.extract.dao;

import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.util.List;
import java.util.Map;

/**
 * MetaDataDAO
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
public interface MetaDataDAO {
    /**
     * health
     *
     * @return health status
     */
    boolean health();

    /**
     * Return metadata loading progress
     *
     * @return metadata loading progress
     */
    MetadataLoadProcess getMetadataLoadProcess();

    /**
     * query table name
     *
     * @return table name list
     */
    List<String> queryTableNameList();

    /**
     * query TableMetadata
     * @return
     */
    List<TableMetadata> queryTableMetadataList();
    
    /**
     * Get table column meta data.
     * @param tableName the table name for query
     * @return the new Metadata.
     */
    TableMetadata queryTableMetadata(String tableName, CheckMode checkMode);
    
    /**
     * Update table column meta data.
     * @param tableMetadata need update table meta data.
     */
    void updateTableColumnMetaData(TableMetadata tableMetadata, CheckMode checkMode);
    /**
     * Query the metadata information of the corresponding column of the table
     *
     * @param tableName tableName
     * @return Column metadata information
     */
    List<ColumnsMetaData> queryTableColumnsMetaData(String tableName);

    /**
     * metadata information to match the row rules
     *
     * @param tableMetadataMap tableMetadataMap
     */
    void matchRowRules(Map<String, TableMetadata> tableMetadataMap);
    
    /**
     * Query the table true lines.
     * @param tableName the tablename
     * @return the table row.
     */
    long queryTableRow(String tableName);
    
    /**
     * Only use when primary key is auto increment.
     * @param tableName
     * @return
     */
    long queryTableMaxId(String tableName, String primaryColumnName);
}
