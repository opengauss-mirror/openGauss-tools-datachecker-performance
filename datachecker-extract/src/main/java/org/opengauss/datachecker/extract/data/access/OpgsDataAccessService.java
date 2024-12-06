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

import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.common.Health;
import org.opengauss.datachecker.common.entry.common.PointPair;
import org.opengauss.datachecker.common.entry.enums.LowerCaseTableNames;
import org.opengauss.datachecker.common.entry.enums.OgCompatibility;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.UniqueColumnBean;
import org.opengauss.datachecker.extract.data.mapper.OpgsMetaDataMapper;

import javax.annotation.PostConstruct;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * OpgsDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public class OpgsDataAccessService extends AbstractDataAccessService {
    private static final String LOWER_CASE_TABLE_NAMES = ConfigConstants.LOWER_CASE_TABLE_NAMES;
    private static final String DOLPHIN_LOWER_CASE_TABLE_NAMES = "dolphin.lower_case_table_names";

    private OpgsMetaDataMapper opgsMetaDataMapper;

    public OpgsDataAccessService(OpgsMetaDataMapper opgsMetaDataMapper) {
        this.opgsMetaDataMapper = opgsMetaDataMapper;
    }

    @Override
    @PostConstruct
    public boolean isOgCompatibilityB() {
        if (!ConfigCache.hasCompatibility()) {
            isOgCompatibilityB = Objects.equals(OgCompatibility.B, opgsMetaDataMapper.sqlCompatibility());
            ConfigCache.put(ConfigConstants.OG_COMPATIBILITY_B, isOgCompatibilityB);
        }
        return ConfigCache.getBooleanValue(ConfigConstants.OG_COMPATIBILITY_B);
    }

    @Override
    public String sqlMode() {
        return isOgCompatibilityB() ? opgsMetaDataMapper.dolphinSqlMode() : opgsMetaDataMapper.sqlMode();
    }

    @Override
    public Health health() {
        String schema = properties.getSchema();
        String sql = "select nspname tableSchema from pg_namespace where nspname='" + schema + "' limit 1";
        return health(schema, sql);
    }

    /**
     * <pre>
     * DAS查询表名列表
     *  select c.relname tableName from pg_class c  LEFT JOIN pg_namespace n on n.oid = c.relnamespace
     *  where n.nspname=? and c.relkind ='r';
     *  </pre>
     *
     * @return tableNameList
     */
    @Override
    public List<String> dasQueryTableNameList() {
        String schema = properties.getSchema();
        String sql = "select c.relname tableName from pg_class c  LEFT JOIN pg_namespace n on n.oid = c.relnamespace "
            + " where n.nspname='" + schema + "' and c.relkind ='r';";
        return adasQueryTableNameList(sql);
    }

    /**
     * <pre>
     *     查询表主键列信息
     *      select c.relname tableName,ns.nspname,ns.oid,a.attname columnName from pg_class c
     *      left join pg_namespace ns on c.relnamespace=ns.oid
     *      left join pg_attribute a on c.oid=a.attrelid and a.attnum>0 and not a.attisdropped
     *      inner join pg_constraint cs on a.attrelid=cs.conrelid and a.attnum=any(cs.conkey)
     *      where ns.nspname='test' and cs.contype='p';
     * </pre>
     *
     * @return primaryColumnList 主键列信息列表
     */
    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns() {
        String schema = properties.getSchema();
        String sql = "select c.relname tableName,ns.nspname,ns.oid,a.attname columnName from pg_class c "
            + "left join pg_namespace ns on c.relnamespace=ns.oid "
            + "left join pg_attribute a on c.oid=a.attrelid and a.attnum>0 and not a.attisdropped "
            + "inner join pg_constraint cs on a.attrelid=cs.conrelid and a.attnum=any(cs.conkey) "
            + "where ns.nspname='" + schema + "' and cs.contype='p';";
        return adasQueryTablePrimaryColumns(sql);
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName) {
        String schema = properties.getSchema();
        String sql = "select c.relname tableName,ns.nspname,ns.oid,a.attname columnName from pg_class c "
            + "left join pg_namespace ns on c.relnamespace=ns.oid "
            + "left join pg_attribute a on c.oid=a.attrelid and a.attnum>0 and not a.attisdropped "
            + "inner join pg_constraint cs on a.attrelid=cs.conrelid and a.attnum=any(cs.conkey) "
            + "where ns.nspname='" + schema + "' and c.relname='" + tableName + "' and cs.contype='p';";
        return adasQueryTablePrimaryColumns(sql);
    }

    @Override
    public List<PrimaryColumnBean> queryTableUniqueColumns(String tableName) {
        String schema = properties.getSchema();
        String sql = "SELECT c.relname AS tableName, ns.nspname, i.indexrelid indexIdentifier, "
            + " a.attname AS columnName, a.attnum colIdx FROM pg_index i"
            + " JOIN pg_class c ON i.indrelid = c.oid join pg_namespace ns on c.relnamespace=ns.oid"
            + " JOIN pg_attribute a ON i.indrelid = a.attrelid AND a.attnum = ANY(i.indkey)         "
            + " where ns.nspname='" + schema + "' and c.relname='" + tableName + "' and i.indisunique = true;";
        List<UniqueColumnBean> uniqueColumns = adasQueryTableUniqueColumns(sql);
        return translateUniqueToPrimaryColumns(uniqueColumns);
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        return opgsMetaDataMapper.queryTableColumnsMetaData(properties.getSchema(), tableName);
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName) {
        return wrapperTableMetadata(opgsMetaDataMapper.queryTableMetadata(properties.getSchema(), tableName));
    }

    @Override
    public List<TableMetadata> dasQueryTableMetadataList() {
        LowerCaseTableNames lowerCaseTableNames = getLowerCaseTableNames();
        String colTableName = Objects.equals(LowerCaseTableNames.SENSITIVE, lowerCaseTableNames)
            ? "c.relname tableName"
            : "lower(c.relname) tableName";
        String sql = " select n.nspname tableSchema, " + colTableName + ",c.reltuples tableRows, "
            + "case when c.reltuples>0 then pg_table_size(c.oid)/c.reltuples else 0 end as avgRowLength "
            + "from pg_class c LEFT JOIN pg_namespace n on n.oid = c.relnamespace " + "where n.nspname='"
            + properties.getSchema() + "' and c.relkind ='r';";
        return wrapperTableMetadata(adasQueryTableMetadataList(sql));
    }

    @Override
    public long rowCount(String tableName) {
        return opgsMetaDataMapper.rowCount(properties.getSchema(), tableName);
    }

    @Override
    public boolean tableExistsRows(String tableName) {
        return opgsMetaDataMapper.tableExistsRows(properties.getSchema(), tableName);
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
        return opgsMetaDataMapper.next(param);
    }

    @Override
    public List<Object> queryPointList(Connection connection, DataAccessParam param) {
        String sql = "select s.%s from ( select row_number() over(order by r.%s  asc) as rn,r.%s  from %s.%s r) s"
            + "  where mod(s.rn, %s ) = 1;";
        sql = String.format(sql, param.getColName(), param.getColName(), param.getColName(), param.getSchema(),
            param.getName(), param.getOffset());
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
        return opgsMetaDataMapper.checkDatabaseNotEmpty(properties.getSchema());
    }

    @Override
    public LowerCaseTableNames queryLowerCaseTableNames() {
        String sql = "SHOW VARIABLES  LIKE \"lower_case_table_names\";";
        Connection connection = getConnection();
        Map<String, LowerCaseTableNames> result = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                LowerCaseTableNames setting = LowerCaseTableNames.codeOf(resultSet.getString("setting"));
                result.put(name, setting);
            }
        } catch (SQLException ex) {
            log.error("queryLowerCaseTableNames error", ex);
        } finally {
            closeConnection(connection);
        }
        return isOgCompatibilityB()
            ? result.getOrDefault(DOLPHIN_LOWER_CASE_TABLE_NAMES, LowerCaseTableNames.UNKNOWN)
            : result.getOrDefault(LOWER_CASE_TABLE_NAMES, LowerCaseTableNames.UNKNOWN);
    }
}
