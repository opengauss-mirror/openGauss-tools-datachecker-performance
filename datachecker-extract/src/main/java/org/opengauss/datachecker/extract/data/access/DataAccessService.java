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

package org.opengauss.datachecker.extract.data.access;

import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.common.Health;
import org.opengauss.datachecker.common.entry.enums.LowerCaseTableNames;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * DataAccessService is top interface of data access service
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public interface DataAccessService {
    /**
     * session sql mode
     *
     * @return sql mode
     */
    String sqlMode();

    /**
     * check jdbc health
     *
     * @return health
     */
    Health health();

    boolean isOgCompatibilityB();

    /**
     * query schema table list
     * filter no primary key tables
     *
     * @return table name list
     */
    List<String> dasQueryTableNameList();

    /**
     * query TableMetadata list
     *
     * @return TableMetadata list
     */
    List<TableMetadata> dasQueryTableMetadataList();

    /**
     * queryTablePrimaryColumns
     *
     * @return primary column
     */
    List<PrimaryColumnBean> queryTablePrimaryColumns();

    /**
     * queryTablePrimaryColumns
     *
     * @param tableName tableName
     * @return primary column
     */
    List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName);

    /**
     * query table ColumnsMetaData
     *
     * @param tableName tableName
     * @return ColumnsMetaData
     */
    List<ColumnsMetaData> queryTableColumnsMetaData(String tableName);

    /**
     * query table TableMetadata
     *
     * @param tableName tableName
     * @return TableMetadata
     */
    TableMetadata queryTableMetadata(String tableName);

    /**
     * query table row count
     *
     * @param tableName tableName
     * @return row count
     */
    long rowCount(String tableName);

    /**
     * query table column min value
     *
     * @param connection connection
     * @param param param
     * @return min value of string
     */
    String min(Connection connection, DataAccessParam param);

    /**
     * query table column max value
     *
     * @param connection connection
     * @param param param
     * @return max value of string
     */
    String max(Connection connection, DataAccessParam param);

    /**
     * query table column next value
     *
     * @param param param
     * @return next value of string
     */
    String next(DataAccessParam param);

    /**
     * query row data by sql
     *
     * @param sql sql
     * @param param sql param
     * @param rowMapper row mapper
     * @param <T> data type
     * @return data
     */
    <T> List<T> query(String sql, Map<String, Object> param, RowMapper<T> rowMapper);

    /**
     * query data from csv file
     *
     * @param table table
     * @param fileName fileName
     * @param differenceList differenceList
     * @return data
     */
    List<Map<String, String>> query(String table, String fileName, List<Difference> differenceList);

    /**
     * get DataSource
     *
     * @return DataSource
     */
    DataSource getDataSource();

    /**
     * query table check point list
     *
     * @param connection connection
     * @param param param
     * @return point list
     */
    List<Object> queryPointList(Connection connection, DataAccessParam param);

    /**
     * check database weather has tables
     *
     * @return boolean
     */
    boolean dasCheckDatabaseNotEmpty();

    /**
     * <pre>
     * query database variables lower_case_table_names
     *  lower_case_table_names=0  : SENSITIVE    : 表名区分大小写
     *  lower_case_table_names=1  : INSENSITIVE  : 表名不区分大小写
     * </pre>
     *
     * @return value
     */
    LowerCaseTableNames queryLowerCaseTableNames();

    /**
     * query table unique columns
     * <pre>
     *     唯一性约束与唯一性索引
     * </pre>
     *
     * @param tableName table
     * @return unique columns
     */
    List<PrimaryColumnBean> queryTableUniqueColumns(String tableName);
}
