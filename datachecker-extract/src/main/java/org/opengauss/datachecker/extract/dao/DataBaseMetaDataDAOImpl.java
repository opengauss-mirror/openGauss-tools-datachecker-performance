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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.service.RuleAdapterService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DataBaseMetaDataDAOImpl
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataBaseMetaDataDAOImpl implements MetaDataDAO {
    private class ConditionBuild {
        public static final String TABLE_NAME = "tableName";
        public static final String DATABASE_SCHEMA = "databaseSchema";
        public static final String TABLE_COLUMN_NAME = "tableColumn";

        private Map<String, Object> param = new HashMap<>();

        public ConditionBuild() {
            this(null, null, null);
        }

        public ConditionBuild(Object schemaVal) {
            this(schemaVal, null, null);
        }

        public ConditionBuild(Object schemaVal, Object tableVal) {
            this(schemaVal, tableVal, null);
        }

        public ConditionBuild(Object schemaVal, Object tableVal, Object columnVal) {
            if (schemaVal != null) {
                putSchema(schemaVal);
            }
            if (tableVal != null) {
                putTableName(tableVal);
            }
            if (columnVal != null) {
                putTableColumn(columnVal);
            }
        }

        public ConditionBuild put(String key, Object val) {
            param.put(key, val);
            return this;
        }

        public ConditionBuild putSchema(Object val) {
            param.put(DATABASE_SCHEMA, val);
            return this;
        }

        public ConditionBuild putTableName(Object val) {
            param.put(TABLE_NAME, val);
            return this;
        }

        public ConditionBuild putTableColumn(Object val) {
            param.put(TABLE_COLUMN_NAME, val);
            return this;
        }

        public Map<String, Object> get() {
            return param;
        }
    }

    protected final JdbcTemplate JdbcTemplateOne;
    private final RuleAdapterService ruleAdapterService;
    private final ExtractProperties extractProperties;
    private volatile MetadataLoadProcess metadataLoadProcess = new MetadataLoadProcess();

    @Override
    public boolean health() {
        String sql = getSql(DataBaseMeta.HEALTH);
        List<String> result = new ArrayList<>();
        JdbcTemplateOne
            .query(sql, (PreparedStatementSetter) ps -> ps.setString(1, getSchema()), new RowCountCallbackHandler() {
                @Override
                protected void processRow(ResultSet rs, int rowNum) throws SQLException {
                    result.add(rs.getString(1));
                }
            });
        return result.size() > 0;
    }

    @Override
    public List<String> queryTableNameList() {
        return filterByTableRules(queryAllTableNames());
    }

    @Override
    public List<TableMetadata> queryTableMetadataList() {
        DataBaseMeta type = DataBaseMeta.TABLE;
        String schema = getSchema();
        Map<String, Object> param = new ConditionBuild(schema).get();
        try {
            Endpoint endpoint = extractProperties.getEndpoint();
            DataBaseType databaseType = extractProperties.getDatabaseType();
            List<TableMetadata> tableList =
                queryByCondition(type, param, (rs, rowNum) -> parseTableMetadata(rs, schema, endpoint, databaseType));
            List<String> tableListNames =
                tableList.stream().map(meta -> meta.getTableName()).collect(Collectors.toList());
            final List<String> tableNameList = filterByTableRules(tableListNames);
            return tableList.stream().filter(meta -> tableNameList.contains(meta.getTableName()))
                            .collect(Collectors.toList());
        } catch (DataAccessException exception) {
            log.error("jdbc query table meta data [{}] error :", type, exception);
        }
        return new LinkedList<>();
    }

    private TableMetadata parseTableMetadata(ResultSet rs, String schema, Endpoint endpoint, DataBaseType dataBaseType)
        throws SQLException {
        return TableMetadata.parse(rs, schema, endpoint, dataBaseType);
    }

    @Override
    public void updateTableColumnMetaData(final TableMetadata tableMetadata, CheckMode checkMode) {
        String tableName = tableMetadata.getTableName();
        final List<ColumnsMetaData> columnsMetaData = queryTableColumnsMetaData(tableName);
        tableMetadata.setColumnsMetas(columnsMetaData);
        tableMetadata.setPrimaryMetas(getTablePrimaryColumn(columnsMetaData));
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName, CheckMode checkMode) {
        Map<String, Object> tableCondition = new ConditionBuild(getSchema(), tableName).get();
        String sql = MetaSqlMapper.getOneTableMetaSql(extractProperties.getDatabaseType());
        TableMetadata tableMetadata = null;
        try {
            List<TableMetadata> tableMetadataList =
                queryByCondition(sql, tableCondition, (rs, rowNum) -> TableMetadata.parse(rs));
            if (tableMetadataList.isEmpty()) {
                return null;
            }
            tableMetadata = tableMetadataList.get(0);
            updateTableColumnMetaData(tableMetadata, checkMode);
        } catch (DataAccessException exception) {
            log.error("jdbc query sub column metadata [{}] error :", sql, exception);
        }
        return tableMetadata;
    }

    private List<ColumnsMetaData> getTablePrimaryColumn(List<ColumnsMetaData> columnsMetaData) {
        return columnsMetaData.stream().filter(meta -> ColumnKey.PRI.equals(meta.getColumnKey()))
                              .sorted(Comparator.comparing(ColumnsMetaData::getOrdinalPosition))
                              .collect(Collectors.toList());
    }

    @Override
    public void matchRowRules(Map<String, TableMetadata> tableMetadataMap) {
        if (MapUtils.isEmpty(tableMetadataMap)) {
            return;
        }
        ruleAdapterService.executeRowRule(tableMetadataMap);
    }

    @Override
    public long queryTableRow(final String tableName) {
        DataBaseMeta type = DataBaseMeta.COUNT;
        String sql = String.format(getSql(type), getSchema(), tableName);
        Map<String, Object> tableCondition = new ConditionBuild().get();
        try {
            return queryByCondition(sql, tableCondition, (rs, rowNum) -> rs.getLong(1)).get(0);
        } catch (DataAccessException exception) {
            log.error("jdbc query table row [{}] error :", type, exception);
        }
        return 0;
    }

    @Override
    public long queryTableMaxId(final String tableName, final String primaryColumnName) {
        DataBaseMeta type = DataBaseMeta.MAX_ID_COUNT;
        String sql = String.format(getSql(type), primaryColumnName, primaryColumnName, getSchema(), tableName);
        Map<String, Object> tableCondition = new ConditionBuild().get();
        try {
            return queryByCondition(sql, tableCondition, (rs, rowNum) -> rs.getLong(1)).get(0);
        } catch (DataAccessException exception) {
            log.error("jdbc query table max id [{}] error :", sql, exception);
        }
        return 0;
    }

    private List<String> queryAllTableNames() {
        Map<String, Object> condition = new ConditionBuild(getSchema()).get();
        DataBaseMeta type = DataBaseMeta.TABLE;
        return queryByCondition(type, condition, (rs, rowNum) -> rs.getString(1));
    }

    private List<String> filterByTableRules(List<String> tableNameList) {
        return ruleAdapterService.executeTableRule(tableNameList);
    }

    @Override
    public MetadataLoadProcess getMetadataLoadProcess() {
        return metadataLoadProcess;
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        DataBaseMeta type = DataBaseMeta.COLUMN;
        Map<String, Object> map = new ConditionBuild(getSchema(), tableName).get();
        try {
            List<ColumnsMetaData> columns = queryByCondition(type, map, (rs, rowNum) -> ColumnsMetaData.parse(rs));
            return ruleAdapterService.executeColumnRule(columns);
        } catch (DataAccessException exception) {
            log.error("jdbc query sub column metadata [{}] error :", type, exception);
        }
        return new LinkedList<>();
    }

    private <T> List<T> queryByCondition(String sql, Map<String, Object> paramMap, RowMapper<T> rowMapper) {
        LocalDateTime start = LocalDateTime.now();
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(JdbcTemplateOne);
        List<T> results = jdbc.query(sql, paramMap, rowMapper);
        log.debug("query sql:<{}> size:{} cost:{}", sql, results.size(),
            Duration.between(start, LocalDateTime.now()).toSeconds());
        return results;
    }

    private <T> List<T> queryByCondition(DataBaseMeta type, Map<String, Object> paramMap, RowMapper<T> rowMapper) {
        return queryByCondition(getSql(type), paramMap, rowMapper);
    }

    private String getSql(DataBaseMeta type) {
        return MetaSqlMapper.getMetaSql(extractProperties.getDatabaseType(), type);
    }

    /**
     * Dynamically obtain the schema information of the current data source
     *
     * @return database schema
     */
    private String getSchema() {
        return extractProperties.getSchema();
    }
}