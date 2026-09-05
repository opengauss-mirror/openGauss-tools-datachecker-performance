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

import com.alibaba.druid.pool.DruidDataSource;

import cn.hutool.core.collection.CollUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.common.Health;
import org.opengauss.datachecker.common.entry.common.PointPair;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.enums.LowerCaseTableNames;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.UniqueColumnBean;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.DurationUtils;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.resource.ConnectionMgr;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import jakarta.annotation.Resource;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * AbstractDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public abstract class AbstractDataAccessService implements DataAccessService {
    protected static final Logger log = LogUtils.getLogger(DataAccessService.class);

    private static final String RS_COL_SCHEMA = "tableSchema";
    private static final String RS_COL_TABLE_NAME = "tableName";
    private static final String RS_COL_TABLE_ROWS = "tableRows";
    private static final String RS_COL_COLUMN_NAME = "columnName";
    private static final String RS_COL_AVG_ROW_LENGTH = "avgRowLength";

    protected boolean isOgCompatibilityB = false;
    @Resource
    protected JdbcTemplate jdbcTemplate;
    @Resource
    protected DruidDataSource druidDataSource;
    @Resource
    protected ExtractProperties properties;

    /**
     * get database connection
     *
     * @return connection
     */
    protected Connection getConnection() {
        return ConnectionMgr.getConnection();
    }

    /**
     * close database connection
     *
     * @param connection connection
     */
    protected void closeConnection(Connection connection) {
        ConnectionMgr.close(connection);
    }

    @Override
    public <T> List<T> query(String sql, Map<String, Object> param, RowMapper<T> rowMapper) {
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        return jdbc.query(sql, param, rowMapper);
    }

    @Override
    public <T> List<T> queryOneWithOffset(String baseSql, long offset, RowMapper<T> rowMapper) {
        String paginatedSql = buildPaginatedSql(baseSql, offset);
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        return jdbc.query(paginatedSql, new HashMap<>(), rowMapper);
    }

    /**
     * Build paginated SQL according to the database type.
     * <ul>
     *   <li>MySQL / openGauss: {@code baseSql LIMIT 1 OFFSET offset}</li>
     *   <li>Oracle: {@code baseSql OFFSET offset ROWS FETCH NEXT 1 ROWS ONLY}</li>
     * </ul>
     */
    private String buildPaginatedSql(String baseSql, long offset) {
        DataBaseType dbType = properties.getDatabaseType();
        if (dbType == DataBaseType.O) {
            return baseSql + " OFFSET " + offset + " ROWS FETCH NEXT 1 ROWS ONLY";
        }
        // Both MySQL (MS) and openGauss (OG) support LIMIT ... OFFSET
        return baseSql + " LIMIT 1 OFFSET " + offset;
    }

    @Override
    public DataSource getDataSource() {
        return druidDataSource;
    }

    /**
     * query whether the schema information exists
     *
     * @param sql schema query sql, bind schema via named parameter :schema
     * @param params named parameters
     * @return result
     */
    public String adasQuerySchema(String sql, Map<String, Object> params) {
        try {
            List<String> result = query(sql, params, (resultSet, rowNum) -> resultSet.getString(RS_COL_SCHEMA));
            return CollUtil.isEmpty(result) ? "" : result.get(0);
        } catch (DataAccessException ex) {
            throw new ExtractDataAccessException("can not access current database");
        }
    }

    /**
     * whether the database schema is valid
     *
     * @param schema schema
     * @param sql sql with :schema named parameter
     * @return result
     */
    public Health health(String schema, String sql) {
        try {
            Connection connection = getConnection();
            if (Objects.isNull(connection)) {
                return Health.buildFailed("can not connection current database");
            }
            closeConnection(connection);
            String result = adasQuerySchema(sql, Map.of("schema", schema));
            if (StringUtils.equalsIgnoreCase(result, schema)) {
                return Health.buildSuccess();
            } else {
                return Health.buildFailed("schema is not exist");
            }
        } catch (ExtractDataAccessException ex) {
            return Health.buildFailed(ex.getMessage());
        }
    }

    /**
     * adasQueryTableNameList
     *
     * @param sql table list query sql with named parameters
     * @param params named parameters
     * @return table list
     */
    public List<String> adasQueryTableNameList(String sql, Map<String, Object> params) {
        final LocalDateTime start = LocalDateTime.now();
        List<String> list = new LinkedList<>();
        try {
            list = query(sql, params, (resultSet, rowNum) -> resultSet.getString(RS_COL_TABLE_NAME));
        } catch (DataAccessException ex) {
            LogUtils.error(log, "{}adasQueryTableNameList error ", ErrorCode.EXECUTE_QUERY_SQL, ex);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        LogUtils.debug(log, "adasQueryTableNameList cost [{}ms]", betweenToMillis);
        return list;
    }

    /**
     * adasQueryTablePrimaryColumns
     *
     * @param sql primary column query sql with named parameters
     * @param params named parameters
     * @return PrimaryColumnBean list
     */
    public List<PrimaryColumnBean> adasQueryTablePrimaryColumns(String sql, Map<String, Object> params) {
        final LocalDateTime start = LocalDateTime.now();
        List<PrimaryColumnBean> list = new LinkedList<>();
        try {
            list = query(sql, params, (resultSet, rowNum) -> {
                PrimaryColumnBean metadata = new PrimaryColumnBean();
                metadata.setColumnName(resultSet.getString(RS_COL_COLUMN_NAME));
                metadata.setTableName(resultSet.getString(RS_COL_TABLE_NAME));
                return metadata;
            });
        } catch (DataAccessException ex) {
            LogUtils.error(log, "{}adasQueryTablePrimaryColumns error:", ErrorCode.EXECUTE_QUERY_SQL, ex);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        LogUtils.debug(log, "adasQueryTablePrimaryColumns cost [{}ms]", betweenToMillis);
        return list;
    }

    /**
     * adas query table unique constraint column info
     *
     * @param sql unique column query sql with named parameters
     * @param params named parameters
     * @return List<UniqueColumnBean>
     */
    public List<UniqueColumnBean> adasQueryTableUniqueColumns(String sql, Map<String, Object> params) {
        List<UniqueColumnBean> list = new LinkedList<>();
        try {
            list = query(sql, params, (resultSet, rowNum) -> {
                UniqueColumnBean metadata = new UniqueColumnBean();
                metadata.setTableName(resultSet.getString("tableName"));
                metadata.setColumnName(resultSet.getString("columnName"));
                metadata.setIndexIdentifier(resultSet.getString("indexIdentifier"));
                metadata.setColIdx(resultSet.getInt("colIdx"));
                return metadata;
            });
        } catch (DataAccessException ex) {
            LogUtils.error(log, "{}adasQueryTablePrimaryColumns error:", ErrorCode.EXECUTE_QUERY_SQL, ex);
        }
        return list;
    }

    /**
     * convert a UniqueColumnBean list to a PrimaryColumnBean list
     *
     * @param uniqueColumns input UniqueColumnBean list, may be empty
     * @return PrimaryColumnBean list, never null, with distinct elements
     */
    public List<PrimaryColumnBean> translateUniqueToPrimaryColumns(List<UniqueColumnBean> uniqueColumns) {
        if (CollUtil.isEmpty(uniqueColumns)) {
            return new ArrayList<>();
        }
        return uniqueColumns.stream()
            .map(u -> new PrimaryColumnBean(u.getTableName(), u.getColumnName()))
            .distinct()
            .collect(Collectors.toList());
    }

    /**
     * adasQueryTableMetadataList
     *
     * @param sql metadata list query sql with named parameters
     * @param params named parameters
     * @return TableMetadata list
     */
    public List<TableMetadata> adasQueryTableMetadataList(String sql, Map<String, Object> params) {
        final LocalDateTime start = LocalDateTime.now();
        List<TableMetadata> list = new LinkedList<>();
        try {
            list = query(sql, params, (resultSet, rowNum) -> mapTableMetadata(resultSet));
        } catch (DataAccessException ex) {
            LogUtils.error(log, "{}adasQueryTableMetadataList error: ", ErrorCode.EXECUTE_QUERY_SQL, ex);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        LogUtils.debug(log, "dasQueryTableMetadataList cost [{}ms]", betweenToMillis);
        return list;
    }

    /**
     * query table metadata
     *
     * @param sql metadata query sql with named parameters
     * @param params named parameters
     * @return metadata
     */
    public TableMetadata adasQueryTableMetadata(String sql, Map<String, Object> params) {
        List<TableMetadata> list = new LinkedList<>();
        try {
            list = query(sql, params, (resultSet, rowNum) -> mapTableMetadata(resultSet));
        } catch (DataAccessException ex) {
            LogUtils.error(log, "{}adasQueryTableMetadata error: ", ErrorCode.EXECUTE_QUERY_SQL, ex);
        }
        return CollUtil.isEmpty(list) ? null : CollUtil.getLast(list);
    }

    private TableMetadata mapTableMetadata(ResultSet resultSet) throws SQLException {
        TableMetadata metadata = new TableMetadata();
        metadata.setSchema(resultSet.getString(RS_COL_SCHEMA));
        metadata.setTableName(resultSet.getString(RS_COL_TABLE_NAME));
        metadata.setTableRows(resultSet.getLong(RS_COL_TABLE_ROWS));
        metadata.setAvgRowLength(resultSet.getLong(RS_COL_AVG_ROW_LENGTH));
        return metadata;
    }

    /**
     * query table data sampling checkpoint list
     *
     * @param connection connection
     * @param sql checkpoint query SQL
     * @return checkpoint list
     */
    protected List<Object> adasQueryPointList(Connection connection, String sql) {
        final LocalDateTime start = LocalDateTime.now();
        List<Object> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                list.add(resultSet.getString(1));
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "{}adasQueryPointList error", ErrorCode.EXECUTE_QUERY_SQL, esql);
        }
        LogUtils.debug(log, "adasQueryPointList [{}] cost [{}ms]", sql, DurationUtils.betweenSeconds(start));
        return list;
    }

    /**
     * query union point list
     *
     * @param connection conn
     * @param sql sql
     * @return point list
     */
    protected List<PointPair> adasQueryUnionPointList(Connection connection, String sql) {
        List<PointPair> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                list.add(new PointPair(resultSet.getString(1), resultSet.getLong(2)));
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "{}adasQueryPointList error", ErrorCode.EXECUTE_QUERY_SQL, esql);
        }
        return list;
    }

    /**
     * Execute a cardinality SQL (e.g. count(distinct)) that returns a single long row;
     * only one row is materialized, with O(1) memory.
     *
     * @param connection connection
     * @param sql sql
     * @return cardinality; -1 on query exception
     */
    protected long adasQueryCardinality(Connection connection, String sql) {
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "{}adasQueryCardinality error", ErrorCode.EXECUTE_QUERY_SQL, esql);
        }
        return -1;
    }

    /**
     * query table data sampling checkpoint list
     *
     * @param connection connection
     * @param sql checkpoint query SQL
     * @return checkpoint list
     */
    protected String adasQueryOnePoint(Connection connection, String sql) {
        final LocalDateTime start = LocalDateTime.now();
        String result = null;
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            if (resultSet.next()) {
                result = resultSet.getString(1);
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "{}adasQueryOnePoint error", ErrorCode.EXECUTE_QUERY_SQL, esql);
        }
        LogUtils.debug(log, "adasQueryPointList [{}] cost [{}ms]", sql, DurationUtils.betweenSeconds(start));
        return result;
    }

    private long durationBetweenToMillis(LocalDateTime start, LocalDateTime end) {
        return Duration.between(start, end).toMillis();
    }

    /**
     * wrapper table metadata of endpoint and databaseType
     *
     * @param tableMetadata tableMetadata
     * @return tableMetadata
     */
    protected TableMetadata wrapperTableMetadata(TableMetadata tableMetadata) {
        if (tableMetadata == null) {
            return null;
        }
        return tableMetadata.setDataBaseType(properties.getDatabaseType())
            .setEndpoint(properties.getEndpoint())
            .setOgCompatibilityB(isOgCompatibilityB);
    }

    /**
     * jdbc mode does not use it
     *
     * @param table table
     * @param fileName fileName
     * @param differenceList differenceList
     * @return result
     */
    @Override
    public List<Map<String, String>> query(String table, String fileName, List<Difference> differenceList) {
        return null;
    }

    /**
     * wrapper table metadata of endpoint and databaseType
     *
     * @param list list of TableMetadata
     * @return tableMetadata
     */
    protected List<TableMetadata> wrapperTableMetadata(List<TableMetadata> list) {
        list.forEach(meta -> meta.setDataBaseType(properties.getDatabaseType())
            .setEndpoint(properties.getEndpoint())
            .setOgCompatibilityB(isOgCompatibilityB));
        return list;
    }

    /**
     * get lowerCaseTableNames
     *
     * @return lowerCaseTableNames
     */
    protected LowerCaseTableNames getLowerCaseTableNames() {
        if (!ConfigCache.hasKey(ConfigConstants.LOWER_CASE_TABLE_NAMES)) {
            LowerCaseTableNames lowerCaseTableNames = queryLowerCaseTableNames();
            ConfigCache.put(ConfigConstants.LOWER_CASE_TABLE_NAMES, lowerCaseTableNames);
        }
        return ConfigCache.getValue(ConfigConstants.LOWER_CASE_TABLE_NAMES, LowerCaseTableNames.class);
    }
}
