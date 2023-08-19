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
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.util.LongHashFunctionWrapper;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.dml.BatchDeleteDmlBuilder;
import org.opengauss.datachecker.extract.dml.DeleteDmlBuilder;
import org.opengauss.datachecker.extract.dml.DmlBuilder;
import org.opengauss.datachecker.extract.dml.InsertDmlBuilder;
import org.opengauss.datachecker.extract.dml.SelectDmlBuilder;
import org.opengauss.datachecker.extract.dml.UpdateDmlBuilder;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DML  Data operation service realizes dynamic query of data
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
@Service
public class DataManipulationService {
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
     * @param tableName     tableName
     * @param compositeKeys compositeKeys
     * @param tableMetadata tableMetadata
     * @return query result
     */
    public List<RowDataHash> queryColumnHashValues(String tableName, List<String> compositeKeys,
        TableMetadata tableMetadata) {
        Assert.isTrue(Objects.nonNull(tableMetadata), "Abnormal table metadata , failed to build select SQL");
        final List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();

        Assert.isTrue(!CollectionUtils.isEmpty(primaryMetas),
            "The metadata information of the table primary key is abnormal, and the construction of select SQL failed");
        final SelectDmlBuilder dmlBuilder = new SelectDmlBuilder(databaseType, tableMetadata.isOgCompatibilityB());
        // Single primary key table data query
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryData = primaryMetas.get(0);
            String querySql = dmlBuilder.dataBaseType(databaseType).schema(extractProperties.getSchema())
                                        .columns(tableMetadata.getColumnsMetas()).tableName(tableName)
                                        .conditionPrimary(primaryData).build();
            return queryColumnValuesSinglePrimaryKey(querySql, compositeKeys, tableMetadata);
        } else {
            // Compound primary key table data query

            String querySql = dmlBuilder.dataBaseType(databaseType).schema(extractProperties.getSchema())
                                        .columns(tableMetadata.getColumnsMetas()).tableName(tableName)
                                        .conditionCompositePrimary(primaryMetas).build();
            List<Object[]> batchParam = dmlBuilder.conditionCompositePrimaryValue(primaryMetas, compositeKeys);
            return queryColumnValuesByCompositePrimary(querySql, batchParam, tableMetadata);
        }
    }

    /**
     * queryColumnValues
     *
     * @param tableName     tableName
     * @param compositeKeys compositeKeys
     * @param metadata      tableMetadata
     * @return query result
     */
    public List<Map<String, String>> queryColumnValues(String tableName, List<String> compositeKeys,
        TableMetadata metadata) {
        Assert.isTrue(Objects.nonNull(metadata), "Abnormal table metadata information, failed to build select SQL");
        final List<ColumnsMetaData> primaryMetas = metadata.getPrimaryMetas();

        Assert.isTrue(!CollectionUtils.isEmpty(primaryMetas),
            "The metadata information of the table primary key is abnormal, and the construction of select SQL failed");
        final SelectDmlBuilder dmlBuilder = new SelectDmlBuilder(databaseType, metadata.isOgCompatibilityB());
        List<Map<String, String>> resultMap;
        // Single primary key table data query
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryData = primaryMetas.get(0);
            String querySql = dmlBuilder.schema(extractProperties.getSchema()).columns(metadata.getColumnsMetas())
                                        .tableName(tableName).conditionPrimary(primaryData).build();
            resultMap = queryColumnValuesSinglePrimaryKey(querySql, compositeKeys);
        } else {
            // Compound primary key table data query
            String querySql = dmlBuilder.schema(extractProperties.getSchema()).columns(metadata.getColumnsMetas())
                                        .tableName(tableName).conditionCompositePrimary(primaryMetas).build();
            List<Object[]> batchParam = dmlBuilder.conditionCompositePrimaryValue(primaryMetas, compositeKeys);
            resultMap = queryColumnValuesByCompositePrimary(querySql, batchParam);
        }
        rectifyValue(metadata, resultMap);
        return resultMap;
    }

    /**
     * Compound primary key table data query
     *
     * @param selectDml     Query SQL
     * @param batchParam    Compound PK query parameters
     * @param tableMetadata tableMetadata
     * @return Query data results
     */
    private List<RowDataHash> queryColumnValuesByCompositePrimary(String selectDml, List<Object[]> batchParam,
        TableMetadata tableMetadata) {
        // Query the current task data and organize the data
        HashMap<String, Object> paramMap = new HashMap<>(InitialCapacity.CAPACITY_1);
        paramMap.put(DmlBuilder.PRIMARY_KEYS, batchParam);
        return queryColumnValues(selectDml, paramMap, tableMetadata);
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
     * @param selectDml     Query SQL
     * @param primaryKeys   Query primary key collection
     * @param tableMetadata tableMetadata
     * @return Query data results
     */
    private List<RowDataHash> queryColumnValuesSinglePrimaryKey(String selectDml, List<String> primaryKeys,
        TableMetadata tableMetadata) {
        // Query the current task data and organize the data
        HashMap<String, Object> paramMap = new HashMap<>(InitialCapacity.CAPACITY_1);
        paramMap.put(DmlBuilder.PRIMARY_KEYS, primaryKeys);
        return queryColumnValues(selectDml, paramMap, tableMetadata);
    }

    private List<Map<String, String>> queryColumnValuesSinglePrimaryKey(String selectDml, List<String> primaryKeys) {
        // Query the current task data and organize the data
        HashMap<String, Object> paramMap = new HashMap<>(InitialCapacity.CAPACITY_1);
        paramMap.put(DmlBuilder.PRIMARY_KEYS, primaryKeys);
        return queryColumnValues(selectDml, paramMap);
    }

    /**
     * Primary key table data query
     *
     * @param selectDml Query SQL
     * @param paramMap  query parameters
     * @return query result
     */
    private List<RowDataHash> queryColumnValues(String selectDml, Map<String, Object> paramMap,
        TableMetadata tableMetadata) {
        List<String> columns = MetaDataUtil.getTableColumns(tableMetadata);
        List<String> primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        // Use JDBC to query the current task to extract data
        ResultSetHandler resultSetHandler = resultSetFactory.createHandler(databaseType);
        return dataAccessService.query(selectDml, paramMap,
            (rs, rowNum) -> resultSetHashHandler.handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs)));
    }

    private List<Map<String, String>> queryColumnValues(String selectDml, Map<String, Object> paramMap) {
        ResultSetHandler handler = resultSetFactory.createHandler(databaseType);
        return dataAccessService.query(selectDml, paramMap, (rs, rowNum) -> handler.putOneResultSetToMap(rs));
    }

    /**
     * Build the replace SQL statement of the specified table
     *
     * @param tableName       tableName
     * @param compositeKeySet composite key set
     * @param metadata        metadata
     * @return Return to SQL list
     */
    public List<String> buildReplace(String schema, String tableName, Set<String> compositeKeySet,
        TableMetadata metadata, boolean ogCompatibility) {
        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        List<Map<String, String>> columnValues = queryColumnValues(tableName, List.copyOf(compositeKeySet), metadata);
        Map<String, Map<String, String>> compositeKeyValues =
            transtlateColumnValues(columnValues, metadata.getPrimaryMetas());
        UpdateDmlBuilder builder = new UpdateDmlBuilder(DataBaseType.OG, ogCompatibility);
        builder.metadata(metadata).tableName(tableName).schema(localSchema);
        compositeKeySet.forEach(compositeKey -> {
            Map<String, String> columnValue = compositeKeyValues.get(compositeKey);
            if (Objects.nonNull(columnValue) && !columnValue.isEmpty()) {
                builder.columnsValues(columnValue);
                resultList.add(builder.build());
            }
        });
        return resultList;
    }

    /**
     * Build the insert SQL statement of the specified table
     *
     * @param tableName       tableName
     * @param compositeKeySet composite key set
     * @param metadata        metadata
     * @return Return to SQL list
     */
    public List<String> buildInsert(String schema, String tableName, Set<String> compositeKeySet,
        TableMetadata metadata, boolean ogCompatibility) {

        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        InsertDmlBuilder builder = new InsertDmlBuilder(DataBaseType.OG, ogCompatibility);
        builder.schema(localSchema).tableName(tableName).columns(metadata.getColumnsMetas());
        List<Map<String, String>> columnValues =
            queryColumnValues(tableName, new ArrayList<>(compositeKeySet), metadata);
        Map<String, Map<String, String>> compositeKeyValues =
            transtlateColumnValues(columnValues, metadata.getPrimaryMetas());
        compositeKeySet.forEach(compositeKey -> {
            Map<String, String> columnValue = compositeKeyValues.get(compositeKey);
            if (Objects.nonNull(columnValue) && !columnValue.isEmpty()) {
                resultList.add(builder.columnsValue(columnValue, metadata.getColumnsMetas()).build());
            }
        });
        return resultList;
    }

    private Map<String, Map<String, String>> transtlateColumnValues(List<Map<String, String>> columnValues,
        List<ColumnsMetaData> primaryMetas) {
        final List<String> primaryKeys = getCompositeKeyColumns(primaryMetas);
        Map<String, Map<String, String>> map = new HashMap<>(InitialCapacity.CAPACITY_16);
        columnValues.forEach(values -> {
            map.put(getCompositeKey(values, primaryKeys), values);
        });
        return map;
    }

    private List<String> getCompositeKeyColumns(List<ColumnsMetaData> primaryMetas) {
        return primaryMetas.stream().map(ColumnsMetaData::getColumnName).collect(Collectors.toUnmodifiableList());
    }

    private String getCompositeKey(Map<String, String> columnValues, List<String> primaryKeys) {
        return primaryKeys.stream().map(key -> columnValues.get(key))
                          .collect(Collectors.joining(ExtConstants.PRIMARY_DELIMITER));
    }

    /**
     * Build a batch delete SQL statement for the specified table
     *
     * @param tableName       tableName
     * @param compositeKeySet composite key set
     * @param primaryMetas    Primary key metadata information
     * @return Return to SQL list
     */
    public List<String> buildBatchDelete(String schema, String tableName, Set<String> compositeKeySet,
        List<ColumnsMetaData> primaryMetas) {
        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryMeta = primaryMetas.stream().findFirst().get();
            compositeKeySet.forEach(compositeKey -> {
                final String deleteDml =
                    new BatchDeleteDmlBuilder().tableName(tableName).schema(localSchema).conditionPrimary(primaryMeta)
                                               .build();
                resultList.add(deleteDml);
            });
        } else {
            compositeKeySet.forEach(compositeKey -> {
                resultList.add(new BatchDeleteDmlBuilder().tableName(tableName).schema(localSchema)
                                                          .conditionCompositePrimary(primaryMetas).build());
            });
        }
        return resultList;
    }

    /**
     * Build the delete SQL statement of the specified table
     *
     * @param tableName       tableName
     * @param compositeKeySet composite key set
     * @param primaryMetas    Primary key metadata information
     * @param ogCompatibility
     * @return Return to SQL list
     */
    public List<String> buildDelete(String schema, String tableName, Set<String> compositeKeySet,
        List<ColumnsMetaData> primaryMetas, boolean ogCompatibility) {

        List<String> resultList = new ArrayList<>();
        final String localSchema = getLocalSchema(schema);
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryMeta = primaryMetas.stream().findFirst().get();
            compositeKeySet.forEach(compositeKey -> {
                DeleteDmlBuilder deleteDmlBuilder = new DeleteDmlBuilder(DataBaseType.OG, ogCompatibility);
                final String deleteDml =
                    deleteDmlBuilder.tableName(tableName).schema(localSchema).condition(primaryMeta, compositeKey)
                                    .build();
                resultList.add(deleteDml);
            });
        } else {
            compositeKeySet.forEach(compositeKey -> {
                DeleteDmlBuilder deleteDmlBuilder = new DeleteDmlBuilder(DataBaseType.OG, ogCompatibility);
                resultList.add(deleteDmlBuilder.tableName(tableName).schema(localSchema)
                                               .conditionCompositePrimary(compositeKey, primaryMetas).build());
            });
        }
        return resultList;
    }

    private String getLocalSchema(String schema) {
        if (StringUtils.isEmpty(schema)) {
            return extractProperties.getSchema();
        }
        return schema;
    }

    /**
     * Query the metadata information of the current table structure and hash
     *
     * @param tableName tableName
     * @return Table structure hash
     */
    public TableMetadataHash queryTableMetadataHash(String tableName) {
        final TableMetadataHash tableMetadataHash = new TableMetadataHash();
        final List<String> allTableNames = metaDataService.queryAllTableNames();
        tableMetadataHash.setTableName(tableName);
        if (allTableNames.contains(tableName)) {
            final List<ColumnsMetaData> columnsMetaData = metaDataService.queryTableColumnMetaDataOfSchema(tableName);
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
