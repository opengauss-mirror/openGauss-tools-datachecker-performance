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
import org.opengauss.datachecker.common.entry.enums.LowerCaseTableNames;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.UniqueColumnBean;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.DurationUtils;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.resource.ConnectionMgr;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.annotation.Resource;
import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
     * 获取数据库连接
     *
     * @return connection
     */
    protected Connection getConnection() {
        return ConnectionMgr.getConnection();
    }

    /**
     * 关闭数据库连接
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
    public DataSource getDataSource() {
        return druidDataSource;
    }

    /**
     * 查询schema 信息是否存在
     *
     * @param executeQueryStatement executeQueryStatement
     * @return result
     */
    public String adasQuerySchema(Connection connection, String executeQueryStatement) {
        String schema = "";
        try (PreparedStatement ps = connection.prepareStatement(executeQueryStatement);
            ResultSet resultSet = ps.executeQuery()) {
            if (resultSet.next()) {
                schema = resultSet.getString(RS_COL_SCHEMA);
            }
        } catch (SQLException esql) {
            throw new ExtractDataAccessException("can not access current database");
        } finally {
            closeConnection(connection);
        }
        return schema;
    }

    /**
     * 数据库schema是否合法
     *
     * @param schema schema
     * @param sql sql
     * @return result
     */
    public Health health(String schema, String sql) {
        try {
            Connection connection = getConnection();
            if (Objects.isNull(connection)) {
                return Health.buildFailed("can not connection current database");
            }
            String result = adasQuerySchema(connection, sql);
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
     * @param executeQueryStatement executeQueryStatement
     * @return table list
     */
    public List<String> adasQueryTableNameList(String executeQueryStatement) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = getConnection();
        List<String> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(executeQueryStatement);
            ResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                list.add(resultSet.getString(RS_COL_TABLE_NAME));
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "adasQueryTableNameList error ", esql);
        } finally {
            closeConnection(connection);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        LogUtils.debug(log, "adasQueryTableNameList cost [{}ms]", betweenToMillis);
        return list;
    }

    /**
     * adasQueryTablePrimaryColumns
     *
     * @param executeQueryStatement executeQueryStatement
     * @return PrimaryColumnBean list
     */
    public List<PrimaryColumnBean> adasQueryTablePrimaryColumns(String executeQueryStatement) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = getConnection();
        List<PrimaryColumnBean> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(executeQueryStatement);
            ResultSet resultSet = ps.executeQuery()) {
            PrimaryColumnBean metadata;
            while (resultSet.next()) {
                metadata = new PrimaryColumnBean();
                metadata.setColumnName(resultSet.getString(RS_COL_COLUMN_NAME));
                metadata.setTableName(resultSet.getString(RS_COL_TABLE_NAME));
                list.add(metadata);
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "adasQueryTablePrimaryColumns error:", esql);
        } finally {
            closeConnection(connection);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        LogUtils.debug(log, "adasQueryTablePrimaryColumns cost [{}ms]", betweenToMillis);
        return list;
    }

    /**
     * adas查询表的唯一性约束列信息
     *
     * @param executeQueryStatement executeQueryStatement
     * @return List<UniqueColumnBean>
     */
    public List<UniqueColumnBean> adasQueryTableUniqueColumns(String executeQueryStatement) {
        Connection connection = getConnection();
        List<UniqueColumnBean> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(executeQueryStatement);
            ResultSet resultSet = ps.executeQuery()) {
            UniqueColumnBean metadata;
            while (resultSet.next()) {
                metadata = new UniqueColumnBean();
                metadata.setTableName(resultSet.getString("tableName"));
                metadata.setColumnName(resultSet.getString("columnName"));
                metadata.setIndexIdentifier(resultSet.getString("indexIdentifier"));
                metadata.setColIdx(resultSet.getInt("colIdx"));
                list.add(metadata);
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "adasQueryTablePrimaryColumns error:", esql);
        } finally {
            closeConnection(connection);
        }
        return list;
    }

    /**
     * 将UniqueColumnBean列表转换为PrimaryColumnBean列表
     *
     * @param uniqueColumns 输入的UniqueColumnBean列表，可能为空
     * @return PrimaryColumnBean列表，永远不会为null，其中的元素是唯一的
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
     * @param executeQueryStatement executeQueryStatement
     * @return TableMetadata list
     */
    public List<TableMetadata> adasQueryTableMetadataList(String executeQueryStatement) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = getConnection();
        List<TableMetadata> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(executeQueryStatement);
            ResultSet resultSet = ps.executeQuery()) {
            TableMetadata metadata;
            while (resultSet.next()) {
                metadata = new TableMetadata();
                metadata.setSchema(resultSet.getString(RS_COL_SCHEMA));
                metadata.setTableName(resultSet.getString(RS_COL_TABLE_NAME));
                metadata.setTableRows(resultSet.getLong(RS_COL_TABLE_ROWS));
                metadata.setAvgRowLength(resultSet.getLong(RS_COL_AVG_ROW_LENGTH));
                list.add(metadata);
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "adasQueryTableMetadataList error: ", esql);
        } finally {
            closeConnection(connection);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        LogUtils.debug(log, "dasQueryTableMetadataList cost [{}ms]", betweenToMillis);
        return list;
    }

    /**
     * 查询表数据抽样检查点清单
     *
     * @param connection connection
     * @param sql 检查点查询SQL
     * @return 检查点列表
     */
    protected List<Object> adasQueryPointList(Connection connection, String sql) {
        final LocalDateTime start = LocalDateTime.now();
        List<Object> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                list.add(resultSet.getString(1));
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "adasQueryPointList error", esql);
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
            LogUtils.error(log, "adasQueryPointList error", esql);
        }
        return list;
    }

    /**
     * 查询表数据抽样检查点清单
     *
     * @param connection connection
     * @param sql 检查点查询SQL
     * @return 检查点列表
     */
    protected String adasQueryOnePoint(Connection connection, String sql) {
        final LocalDateTime start = LocalDateTime.now();
        String result = null;
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            if (resultSet.next()) {
                result = resultSet.getString(1);
            }
        } catch (SQLException esql) {
            LogUtils.error(log, "adasQueryOnePoint error", esql);
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
