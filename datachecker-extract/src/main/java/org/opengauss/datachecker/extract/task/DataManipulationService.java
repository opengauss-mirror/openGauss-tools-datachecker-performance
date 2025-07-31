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

package org.opengauss.datachecker.extract.task;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.LongHashFunctionWrapper;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.dml.DmlBuilder;
import org.opengauss.datachecker.extract.dml.SelectDmlBuilder;
import org.opengauss.datachecker.extract.resource.ConnectionMgr;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * DML  Data operation service realizes dynamic query of data
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
@Service
public class DataManipulationService {
    private static final Logger log = LogUtils.getLogger(DataManipulationService.class);
    private static final LongHashFunctionWrapper HASH_UTIL = new LongHashFunctionWrapper();

    private final ResultSetHashHandler resultSetHashHandler = new ResultSetHashHandler();
    private final ResultSetHandlerFactory resultSetFactory = new ResultSetHandlerFactory();
    @Value("${spring.extract.databaseType}")
    private DataBaseType databaseType;
    @Resource
    private DataAccessService dataAccessService;
    @Resource
    private MetaDataService metaDataService;
    @Resource
    private ExtractProperties extractProperties;

    /**
     * queryColumnValues
     *
     * @param tableName tableName
     * @param compositeKeys compositeKeys
     * @param tableMetadata tableMetadata
     * @return query result
     */
    public List<RowDataHash> queryColumnHashValues(String tableName, List<String> compositeKeys,
        TableMetadata tableMetadata) {
        Assert.isTrue(Objects.nonNull(tableMetadata), "Abnormal table metadata , failed to build select SQL");
        final List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();
        Assert.isTrue(!CollectionUtils.isEmpty(primaryMetas),
            "The metadata of the table primary is abnormal, , failed to build select SQL");
        final SelectDmlBuilder dmlBuilder = new SelectDmlBuilder(databaseType, tableMetadata.isOgCompatibilityB());
        // Single primary key table data query
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryData = primaryMetas.get(0);
            String querySql = dmlBuilder.dataBaseType(databaseType)
                .schema(extractProperties.getSchema())
                .columns(tableMetadata.getColumnsMetas())
                .tableName(tableName)
                .conditionPrimary(primaryData)
                .build();
            return queryColumnValuesSinglePrimaryKey(querySql, compositeKeys, tableMetadata);
        } else {
            // Compound primary key table data query
            String querySql = dmlBuilder.dataBaseType(databaseType)
                .schema(extractProperties.getSchema())
                .columns(tableMetadata.getColumnsMetas())
                .tableName(tableName)
                .conditionCompositePrimary(primaryMetas)
                .build();
            List<Object[]> batchParam = dmlBuilder.conditionCompositePrimaryValue(primaryMetas, compositeKeys);
            return queryColumnValuesByCompositePrimary(querySql, batchParam, tableMetadata);
        }
    }

    /**
     * queryColumnValues
     *
     * @param tableName tableName
     * @param compositeKeys compositeKeys
     * @param metadata tableMetadata
     * @return query result
     */
    public List<Map<String, String>> queryColumnValues(String tableName, List<String> compositeKeys,
        TableMetadata metadata) {
        Assert.isTrue(Objects.nonNull(metadata), "Abnormal table metadata information, failed to build select SQL");
        final List<ColumnsMetaData> primaryMetas = metadata.getPrimaryMetas();
        Assert.isTrue(!CollectionUtils.isEmpty(primaryMetas),
            "The metadata of the table primary is abnormal, failed to build select SQL");
        final SelectDmlBuilder dmlBuilder = new SelectDmlBuilder(databaseType, metadata.isOgCompatibilityB());
        List<Map<String, String>> resultMap;
        // Single primary key table data query
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryData = primaryMetas.get(0);
            String querySql = dmlBuilder.schema(extractProperties.getSchema())
                .columns(metadata.getColumnsMetas())
                .tableName(tableName)
                .conditionPrimary(primaryData)
                .build();
            resultMap = queryColumnValuesSinglePrimaryKey(querySql, compositeKeys);
        } else {
            // Compound primary key table data query
            String querySql = dmlBuilder.schema(extractProperties.getSchema())
                .columns(metadata.getColumnsMetas())
                .tableName(tableName)
                .conditionCompositePrimary(primaryMetas)
                .build();
            List<Object[]> batchParam = dmlBuilder.conditionCompositePrimaryValue(primaryMetas, compositeKeys);
            resultMap = queryColumnValuesByCompositePrimary(querySql, batchParam);
        }
        rectifyValue(metadata, resultMap);
        return resultMap;
    }

    /**
     * Compound primary key table data query
     *
     * @param statement Query SQL
     * @param batchParam Compound PK query parameters
     * @param tableMetadata tableMetadata
     * @return Query data results
     */
    private List<RowDataHash> queryColumnValuesByCompositePrimary(String statement, List<Object[]> batchParam,
        TableMetadata tableMetadata) {
        List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();
        StringBuilder compositeKeyBuilder = new StringBuilder();
        for (int idx = 0; idx < batchParam.size(); idx++) {
            Object[] priValues = batchParam.get(idx);
            compositeKeyBuilder.append("(");
            for (int i = 0; i < priValues.length; i++) {
                Object value = priValues[i];
                ColumnsMetaData meta = primaryMetas.get(i);
                if (MetaDataUtil.isDigitPrimaryKey(meta)) {
                    compositeKeyBuilder.append(value);
                } else {
                    String escapedValue = value.toString().replace("'", "''");
                    compositeKeyBuilder.append("'").append(escapedValue).append("'");
                }
                if (i < priValues.length - 1) {
                    compositeKeyBuilder.append(",");
                }
            }
            compositeKeyBuilder.append(")");
            if (idx < batchParam.size() - 1) {
                compositeKeyBuilder.append(",");
            }
        }
        String safeSql = statement.replace(":primaryKeys", compositeKeyBuilder.toString());
        return statementQuery(safeSql, tableMetadata);
    }

    private void rectifyValue(TableMetadata metadata, List<Map<String, String>> resultMap) {
        List<ColumnsMetaData> columnsMetas = metadata.getColumnsMetas();
        for (ColumnsMetaData columnsMetaData : columnsMetas) {
            if ("tsquery".equals(columnsMetaData.getDataType()) || "tsvector".equals(columnsMetaData.getDataType())) {
                for (Map<String, String> valueMap : resultMap) {
                    String originString = valueMap.get(columnsMetaData.getColumnName());
                    valueMap.put(columnsMetaData.getColumnName(), originString.replaceAll("\'", " "));
                }
            }
            if ("bytea".equals(columnsMetaData.getDataType())) {
                for (Map<String, String> valueMap : resultMap) {
                    String originString = valueMap.get(columnsMetaData.getColumnName());
                    valueMap.put(columnsMetaData.getColumnName(), "\\x" + originString);
                }
            }
        }
    }

    private List<Map<String, String>> queryColumnValuesByCompositePrimary(String selectDml, List<Object[]> batchParam) {
        // Query the current task data and organize the data
        HashMap<String, Object> paramMap = new HashMap<>(InitialCapacity.CAPACITY_1);
        paramMap.put(DmlBuilder.PRIMARY_KEYS, batchParam);
        return queryColumnValues(selectDml, paramMap);
    }

    /**
     * Single primary key table data query
     *
     * @param statement Query SQL
     * @param primaryKeys Query primary key collection
     * @param tableMetadata tableMetadata
     * @return Query data results
     */
    private List<RowDataHash> queryColumnValuesSinglePrimaryKey(String statement, List<String> primaryKeys,
        TableMetadata tableMetadata) {
        return statementQuery(statement.replace(":primaryKeys", String.join(",", primaryKeys)), tableMetadata);
    }

    private List<Map<String, String>> queryColumnValuesSinglePrimaryKey(String selectDml, List<String> primaryKeys) {
        // Query the current task data and organize the data
        HashMap<String, Object> paramMap = new HashMap<>(InitialCapacity.CAPACITY_1);
        paramMap.put(DmlBuilder.PRIMARY_KEYS, primaryKeys);
        return queryColumnValues(selectDml, paramMap);
    }

    private List<RowDataHash> statementQuery(String pageStatement, Map<String, Object> paramMap,
        TableMetadata tableMetadata) {
        Object primaryKeys = paramMap.get("primaryKeys");
        String sqlParam = "";
        try {
            if (primaryKeys instanceof List) {
                List<String> primaryKeyList = (List<String>) primaryKeys;
                sqlParam = String.join(",", primaryKeyList);
            } else {
                throw new IllegalArgumentException("primaryKeys must be List");
            }
        } catch (ClassCastException ex) {
            log.error("{}Failed to query data {}", ErrorCode.EXECUTE_QUERY_SQL, pageStatement, ex);
            throw new IllegalArgumentException("paramMap must be List");
        }
        return statementQuery(pageStatement.replace(":primaryKeys", sqlParam), tableMetadata);
    }

    private List<RowDataHash> statementQuery(String pageStatement, TableMetadata tableMetadata) {
        List<RowDataHash> result = new ArrayList<>();
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            ResultSetHandler handler = resultSetFactory.createHandler(databaseType);
            List<String> tablePrimaryColumns = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
            List<String> tableColumns = MetaDataUtil.getTableColumns(tableMetadata);
            connection = dataAccessService.getDataSource().getConnection();
            ps = connection.prepareStatement(pageStatement);
            resultSet = ps.executeQuery();
            while (resultSet.next()) {
                Map<String, String> rowResult = handler.putOneResultSetToMap(resultSet);
                result.add(resultSetHashHandler.handler(tablePrimaryColumns, tableColumns, rowResult));
            }
        } catch (SQLException | ExtractException ex) {
            log.error("execute query error, sql:{}", pageStatement, ex);
        } finally {
            ConnectionMgr.close(connection, ps, resultSet);
        }
        return result;
    }

    private List<Map<String, String>> queryColumnValues(String selectDml, Map<String, Object> paramMap) {
        ResultSetHandler handler = resultSetFactory.createHandler(databaseType);
        return dataAccessService.query(selectDml, paramMap, (rs, rowNum) -> handler.putOneResultSetToMap(rs));
    }

    /**
     * Query the metadata information of the current table structure and hash
     *
     * @param tableName tableName
     * @return Table structure hash
     */
    public TableMetadataHash queryTableMetadataHash(String tableName) {
        final TableMetadataHash tableMetadataHash = new TableMetadataHash();
        tableMetadataHash.setTableName(tableName);
        TableMetadata tableMetadata = dataAccessService.queryTableMetadata(tableName);
        if (Objects.nonNull(tableMetadata)) {
            List<ColumnsMetaData> columnsMetaData = metaDataService.queryTableColumnMetaDataOfSchema(tableName);
            StringBuffer buffer = new StringBuffer();
            columnsMetaData.sort(Comparator.comparing(ColumnsMetaData::getOrdinalPosition));
            columnsMetaData.forEach(column -> {
                buffer.append(column.getColumnName()).append(column.getOrdinalPosition());
            });
            tableMetadataHash.setTableHash(HASH_UTIL.hashBytes(buffer.toString()));
        } else {
            tableMetadataHash.setTableHash(-1L);
        }
        return tableMetadataHash;
    }
}
