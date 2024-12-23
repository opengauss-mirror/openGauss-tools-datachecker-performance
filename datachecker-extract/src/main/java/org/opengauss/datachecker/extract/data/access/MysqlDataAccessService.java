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
import org.opengauss.datachecker.common.entry.common.PointPair;
import org.opengauss.datachecker.common.entry.enums.LowerCaseTableNames;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.UniqueColumnBean;
import org.opengauss.datachecker.extract.data.mapper.MysqlMetaDataMapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

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
        String sql = "select info.table_name tableName from information_schema.tables info where table_schema='"
            + schema + "'  and table_type='BASE TABLE'";
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
    public List<PrimaryColumnBean> queryTableUniqueColumns(String tableName) {
        String schema = properties.getSchema();
        String sql = "select s.table_schema,s.table_name tableName,s.column_name columnName,c.ordinal_position colIdx,"
            + " s.index_name indexIdentifier from information_schema.statistics s "
            + " left join information_schema.columns c on s.table_schema=c.table_schema  "
            + " and s.table_schema=c.table_schema and s.table_name=c.table_name and s.column_name=c.column_name "
            + " where s.table_schema='" + schema + "' and s.table_name='" + tableName + "'" + " and s.non_unique=0;";
        List<UniqueColumnBean> uniqueColumns = adasQueryTableUniqueColumns(sql);
        return translateUniqueToPrimaryColumns(uniqueColumns);
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName) {
        return mysqlMetaDataMapper.queryTablePrimaryColumnsByTableName(properties.getSchema(), tableName);
    }

    @Override
    public List<TableMetadata> dasQueryTableMetadataList() {
        LowerCaseTableNames lowerCaseTableNames = getLowerCaseTableNames();
        String colTableName = Objects.equals(LowerCaseTableNames.SENSITIVE, lowerCaseTableNames)
            ? "info.table_name tableName"
            : "lower(info.table_name) tableName";
        String sql = " SELECT info.TABLE_SCHEMA tableSchema," + colTableName + ",info.table_rows tableRows , "
            + "info.avg_row_length avgRowLength FROM information_schema.tables info WHERE TABLE_SCHEMA='"
            + properties.getSchema() + "'";
        return wrapperTableMetadata(adasQueryTableMetadataList(sql));
    }

    @Override
    public long rowCount(String tableName) {
        return mysqlMetaDataMapper.rowCount(properties.getSchema(), tableName);
    }

    @Override
    public boolean tableExistsRows(String tableName) {
        return mysqlMetaDataMapper.tableExistsRows(properties.getSchema(), tableName);
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
            + "  (select @rowno := 0) t ORDER BY r.%s asc) s where mod(s.rn, %s) = 1";
        sql = String.format(sql, param.getColName(), param.getColName(), param.getSchema(), param.getName(),
            param.getColName(), param.getOffset());
        return adasQueryPointList(connection, sql);
    }

    @Override
    public List<PointPair> queryUnionFirstPrimaryCheckPointList(Connection connection, DataAccessParam param) {
        String sqlTmp = "select %s,count(1) from %s.%s group by %s";
        String sql = String.format(sqlTmp, param.getColName(), param.getSchema(), param.getName(), param.getColName());
        return adasQueryUnionPointList(connection, sql);
    }

    @Override
    public boolean dasCheckDatabaseNotEmpty() {
        return mysqlMetaDataMapper.checkDatabaseNotEmpty(properties.getSchema());
    }

    @Override
    public LowerCaseTableNames queryLowerCaseTableNames() {
        String sql = "SHOW VARIABLES  LIKE 'lower_case_table_names';";
        Connection connection = getConnection();
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            if (resultSet.next()) {
                String value = resultSet.getString("value");
                return LowerCaseTableNames.codeOf(value);
            }
        } catch (SQLException ex) {
            log.error("queryLowerCaseTableNames error", ex);
        } finally {
            closeConnection(connection);
        }
        return LowerCaseTableNames.UNKNOWN;
    }
}
