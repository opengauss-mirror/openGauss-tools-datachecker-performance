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
import org.opengauss.datachecker.common.entry.common.Health;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.data.mapper.MysqlMetaDataMapper;

import java.sql.Connection;
import java.util.List;

/**
 * MysqlDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public class MysqlDataAccessService extends AbstractDataAccessService {
    private MysqlMetaDataMapper mysqlMetaDataMapper;

    public MysqlDataAccessService(MysqlMetaDataMapper mysqlMetaDataMapper) {
        this.mysqlMetaDataMapper = mysqlMetaDataMapper;
    }

    @Override
    public String sqlMode() {
        return mysqlMetaDataMapper.sqlMode();
    }

    @Override
    public Health health() {
        String schema = properties.getSchema();
        String sql = "SELECT SCHEMA_NAME tableSchema FROM information_schema.SCHEMATA info WHERE SCHEMA_NAME='" + schema
            + "' limit 1";
        return health(schema, sql);
    }

    @Override
    public boolean isOgCompatibilityB() {
        return false;
    }

    @Override
    public List<String> dasQueryTableNameList() {
        String schema = properties.getSchema();
        String sql =
            "SELECT info.table_name tableName FROM information_schema.tables info WHERE table_schema='" + schema + "'";
        return adasQueryTableNameList(sql);
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        return mysqlMetaDataMapper.queryTableColumnsMetaData(properties.getSchema(), tableName);
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName) {
        return wrapperTableMetadata(mysqlMetaDataMapper.queryTableMetadata(properties.getSchema(), tableName));
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns() {
        String sql = "select table_name tableName ,lower(column_name) columnName from information_schema.columns "
            + "where table_schema='" + properties.getSchema() + "' and column_key='PRI' order by ordinal_position asc ";
        return adasQueryTablePrimaryColumns(sql);
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName) {
        return mysqlMetaDataMapper.queryTablePrimaryColumnsByTableName(properties.getSchema(), tableName);
    }

    @Override
    public List<TableMetadata> dasQueryTableMetadataList() {
        String sql = " SELECT info.TABLE_SCHEMA tableSchema,info.table_name tableName,info.table_rows tableRows , "
            + "info.avg_row_length avgRowLength FROM information_schema.tables info WHERE TABLE_SCHEMA='"
            + properties.getSchema() + "'";
        return wrapperTableMetadata(adasQueryTableMetadataList(sql));
    }

    @Override
    public long rowCount(String tableName) {
        return mysqlMetaDataMapper.rowCount(properties.getSchema(), tableName);
    }

    @Override
    public String min(Connection connection, DataAccessParam param) {
        String sql = " select min(" + param.getColName() + ") from " + param.getSchema() + "." + param.getName() + ";";
        return adasQueryOnePoint(connection, sql);
    }

    @Override
    public String max(Connection connection, DataAccessParam param) {
        String sql = " select max(" + param.getColName() + ") from " + param.getSchema() + "." + param.getName() + ";";
        return adasQueryOnePoint(connection, sql);
    }

    @Override
    public String next(DataAccessParam param) {
        return mysqlMetaDataMapper.next(param);
    }

    @Override
    public List<Object> queryPointList(Connection connection, DataAccessParam param) {
        String sql = "select s.%s from (SELECT @rowno:=@rowno+1 as rn,r.%s from %s.%s r,"
            + "  (select @rowno := 0) t ORDER BY r.%s asc) s where mod(s.rn, %s) = 0";
        sql = String.format(sql, param.getColName(), param.getColName(), param.getSchema(), param.getName(),
            param.getColName(), param.getOffset());
        return adasQueryPointList(connection, sql);
    }

    @Override
    public boolean dasCheckDatabaseNotEmpty() {
        return mysqlMetaDataMapper.checkDatabaseNotEmpty(properties.getSchema());
    }
}
