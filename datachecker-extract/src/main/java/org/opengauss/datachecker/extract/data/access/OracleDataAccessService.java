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

import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.data.mapper.OracleMetaDataMapper;

import java.sql.Connection;
import java.util.List;

/**
 * OracleDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public class OracleDataAccessService extends AbstractDataAccessService {
    private OracleMetaDataMapper oracleMetaDataMapper;

    public OracleDataAccessService(OracleMetaDataMapper oracleMetaDataMapper) {
        this.oracleMetaDataMapper = oracleMetaDataMapper;
    }

    @Override
    public String sqlMode() {
        return null;
    }

    @Override
    public boolean health() {
        return oracleMetaDataMapper.health();
    }

    @Override
    public boolean isOgCompatibilityB() {
        return false;
    }

    @Override
    public List<String> dasQueryTableNameList() {
        String schema = properties.getSchema();
        String sql = "SELECT TABLE_NAME tableName FROM ALL_TABLES WHERE OWNER = '" + schema + "'";
        return adasQueryTableNameList(sql);
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        return oracleMetaDataMapper.queryTableColumnsMetaData(properties.getSchema(), tableName);
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName) {
        return wrapperTableMetadata(oracleMetaDataMapper.queryTableMetadata(properties.getSchema(), tableName));
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns() {
        String sql = "SELECT A.TABLE_NAME tableName, A.COLUMN_NAME columnName FROM ALL_CONS_COLUMNS A,ALL_CONSTRAINTS B"
            + " WHERE A.constraint_name = B.constraint_name AND  B.constraint_type = 'P' AND A.OWNER = '"
            + properties.getSchema() + "'";
        return adasQueryTablePrimaryColumns(sql);
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName) {
        return oracleMetaDataMapper.queryTablePrimaryColumnsByTableName(properties.getSchema(), tableName);
    }

    @Override
    public List<TableMetadata> dasQueryTableMetadataList() {
        String schema = properties.getSchema();
        String sql = "SELECT t.owner tableSchema,t.table_name tableName,t.num_rows tableRows,avg_row_len avgRowLength"
            + " FROM ALL_TABLES t LEFT JOIN (SELECT DISTINCT table_name from ALL_CONSTRAINTS where OWNER = '" + schema
            + "' AND constraint_type='P') pc on t.table_name=pc.table_name WHERE t.OWNER = '" + schema + "'";
        return wrapperTableMetadata(adasQueryTableMetadataList(sql));
    }

    @Override
    public long rowCount(String tableName) {
        return oracleMetaDataMapper.rowCount(properties.getSchema(), tableName);
    }

    @Override
    public String min(Connection connection, DataAccessParam param) {
        return oracleMetaDataMapper.min(param);
    }

    @Override
    public String max(Connection connection, DataAccessParam param) {
        return oracleMetaDataMapper.max(param);
    }

    @Override
    public String next(DataAccessParam param) {
        return oracleMetaDataMapper.next(param);
    }

    @Override
    public List<Object> queryPointList(Connection connection, DataAccessParam param) {
        return oracleMetaDataMapper.queryPointList(param);
    }

    @Override
    public boolean dasCheckDatabaseNotEmpty() {
        return oracleMetaDataMapper.checkDatabaseNotEmpty(properties.getSchema());
    }
}
