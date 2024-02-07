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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * AbstractDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public abstract class AbstractDataAccessService implements DataAccessService {
    protected static final Logger log = LogUtils.getLogger();

    private static final String RS_COL_SCHEMA = "tableSchema";
    private static final String RS_COL_TABLE_NAME = "tableName";
    private static final String RS_COL_TABLE_ROWS = "tableRows";
    private static final String RS_COL_COLUMN_NAME = "columnName";
    private static final String RS_COL_AVG_ROW_LENGTH = "avgRowLength";

    protected boolean isOgCompatibilityB = false;
    @Resource
    protected JdbcTemplate jdbcTemplate;
    @Resource
    protected ExtractProperties properties;

    @Override
    public <T> List<T> query(String sql, Map<String, Object> param, RowMapper<T> rowMapper) {
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        return jdbc.query(sql, param, rowMapper);
    }

    @Override
    public DataSource getDataSource() {
        return jdbcTemplate.getDataSource();
    }

    /**
     * adasQueryTableNameList
     *
     * @param executeQueryStatement executeQueryStatement
     * @return table list
     */
    public List<String> adasQueryTableNameList(String executeQueryStatement) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = ConnectionMgr.getConnection();
        List<String> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(executeQueryStatement);
            ResultSet resultSet = ps.executeQuery()) {
            while (resultSet.next()) {
                list.add(resultSet.getString(RS_COL_TABLE_NAME));
            }
        } catch (SQLException esql) {
            log.error("", esql);
        } finally {
            ConnectionMgr.close(connection, null, null);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        log.info("adasQueryTableNameList cost [{}ms]", betweenToMillis);
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
        Connection connection = ConnectionMgr.getConnection();
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
            log.error("", esql);
        } finally {
            ConnectionMgr.close(connection, null, null);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        log.info("adasQueryTablePrimaryColumns cost [{}ms]", betweenToMillis);
        return list;
    }

    /**
     * adasQueryTableMetadataList
     *
     * @param executeQueryStatement executeQueryStatement
     * @return TableMetadata list
     */
    public List<TableMetadata> adasQueryTableMetadataList(String executeQueryStatement) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = ConnectionMgr.getConnection();
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
            log.error("", esql);
        } finally {
            ConnectionMgr.close(connection, null, null);
        }
        long betweenToMillis = durationBetweenToMillis(start, LocalDateTime.now());
        log.info("dasQueryTableMetadataList cost [{}ms]", betweenToMillis);
        return list;
    }

    private long durationBetweenToMillis(LocalDateTime start, LocalDateTime end) {
        return Duration.between(start, end)
                       .toMillis();
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
     * @param table          table
     * @param fileName       fileName
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
}
