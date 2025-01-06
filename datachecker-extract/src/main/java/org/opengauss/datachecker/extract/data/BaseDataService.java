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

package org.opengauss.datachecker.extract.data;

import com.alibaba.druid.pool.DruidDataSource;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.LongHashFunctionWrapper;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.service.RuleAdapterService;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * BaseDataService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
@Service
public class BaseDataService {
    private static final Logger log = LogUtils.getLogger(BaseDataService.class);
    private static final LongHashFunctionWrapper HASH_UTIL = new LongHashFunctionWrapper();

    @Resource
    private DataAccessService dataAccessService;
    @Resource
    private RuleAdapterService ruleAdapterService;
    private DruidDataSource dynamicProxyDataSource;
    private final List<String> tableNameList = new LinkedList<>();

    public DataAccessService getDataAccessService() {
        return dataAccessService;
    }

    /**
     * 初始化动态代理数据源
     */
    public void initDynamicProxyDataSource(DruidDataSource dataSource) {
        dynamicProxyDataSource = dataSource;
    }

    /**
     * get data source
     *
     * @return datasource
     */
    public DruidDataSource getDataSource() {
        return dynamicProxyDataSource;
    }

    /**
     * query check table list , and use rule.table
     * filter no primary key tables,and filter table rule( black and write list)
     *
     * @return table name list
     */
    public synchronized List<String> bdsQueryTableNameList() {
        if (CollectionUtils.isEmpty(tableNameList)) {
            tableNameList.addAll(filterByTableRules(dataAccessService.dasQueryTableNameList()));
        }
        return tableNameList;
    }

    /**
     * query check table metadata list, and use rule.table
     * it is not have any columns and primary columns
     *
     * @return table name list
     */
    public List<TableMetadata> bdsQueryTableMetadataList() {
        try {
            List<TableMetadata> metadataList = dataAccessService.dasQueryTableMetadataList();
            return metadataList.stream().filter(meta -> {
                boolean isChecking = ruleAdapterService.filterTableByRule(meta.getTableName());
                if (isChecking) {
                    tableNameList.add(meta.getTableName());
                }
                meta.setExistTableRows(dataAccessService.tableExistsRows(meta.getTableName()));
                return isChecking;
            }).collect(Collectors.toList());
        } catch (BadSqlGrammarException ex) {
            log.error("{}query table metadata list error {}", ErrorCode.EXECUTE_QUERY_SQL, ex.getMessage());
            throw new ExtractException("query table metadata list error " + ex.getMessage());
        }
    }

    /**
     * query current db of all table primary column
     *
     * @return <table , List[primary column] >
     */
    public Map<String, List<PrimaryColumnBean>> queryTablePrimaryColumns() {
        List<PrimaryColumnBean> columnBeanList = dataAccessService.queryTablePrimaryColumns();
        if (CollectionUtils.isEmpty(columnBeanList)) {
            return new HashMap<>();
        }
        return columnBeanList.stream().collect(Collectors.groupingBy(PrimaryColumnBean::getTableName));
    }

    private List<String> filterByTableRules(List<String> tableNameList) {
        return ruleAdapterService.executeTableRule(tableNameList);
    }

    /**
     * filter table rule
     *
     * @param tableMetadataMap table metadata
     */
    public void matchRowRules(Map<String, TableMetadata> tableMetadataMap) {
        if (MapUtils.isEmpty(tableMetadataMap)) {
            return;
        }
        ruleAdapterService.executeRowRule(tableMetadataMap);
    }

    /**
     * get table metadata from cache ;
     * if cache does not has current table ,query it and add in cache;
     *
     * @param tableName tableName
     * @return TableMetadata
     */
    public TableMetadata getTableMetadata(String tableName) {
        if (!tableNameList.contains(tableName)) {
            return null;
        }
        TableMetadata tableMetadata = MetaDataCache.get(tableName);
        if (Objects.nonNull(tableMetadata)) {
            return tableMetadata;
        }
        tableMetadata = queryTableMetadata(tableName);
        if (Objects.nonNull(tableMetadata)) {
            MetaDataCache.put(tableName, tableMetadata);
        }
        return tableMetadata;
    }

    /**
     * query checked table metadata by tableName,use rule.column
     *
     * @param tableName tableName
     * @return TableMetadata
     */
    public TableMetadata queryTableMetadata(String tableName) {
        if (MetaDataCache.containsKey(tableName)) {
            return MetaDataCache.get(tableName);
        }
        TableMetadata tableMetadata = dataAccessService.queryTableMetadata(tableName);
        if (Objects.isNull(tableMetadata)) {
            return tableMetadata;
        }
        tableMetadata.setExistTableRows(dataAccessService.tableExistsRows(tableName));
        updateTableColumnMetaData(tableMetadata, null);
        LogUtils.debug(log, "query table metadata {} -- {} ", tableName, tableMetadata);
        MetaDataCache.put(tableName, tableMetadata);
        return tableMetadata;
    }

    /**
     * update table metadata, and filter column rules
     *
     * @param tableMetadata table metadata
     * @param primaryColumnBeans primary column
     */
    public void updateTableColumnMetaData(TableMetadata tableMetadata, List<PrimaryColumnBean> primaryColumnBeans) {
        String tableName = tableMetadata.getTableName();
        final List<ColumnsMetaData> columns = dataAccessService.queryTableColumnsMetaData(tableName);
        if (CollectionUtils.isEmpty(columns)) {
            LogUtils.error(log, "{}table columns metadata is null ,{}", ErrorCode.TABLE_COL_NULL, tableName);
            return;
        }
        List<PrimaryColumnBean> tempPrimaryColumnBeans = primaryColumnBeans;
        if (CollectionUtils.isEmpty(primaryColumnBeans)) {
            tempPrimaryColumnBeans = dataAccessService.queryTablePrimaryColumns(tableName);
        }
        if (CollectionUtils.isEmpty(tempPrimaryColumnBeans)) {
            tempPrimaryColumnBeans = dataAccessService.queryTableUniqueColumns(tableName);
        }
        if (CollectionUtils.isNotEmpty(tempPrimaryColumnBeans)) {
            List<String> primaryColumnNameList = getPrimaryColumnNames(tempPrimaryColumnBeans);
            for (ColumnsMetaData column : columns) {
                column.setSchema(tableMetadata.getSchema());
                if (primaryColumnNameList.contains(column.getLowerCaseColumnName())) {
                    column.setColumnKey(ColumnKey.PRI);
                }
            }
        }
        tableMetadata.setColumnsMetas(ruleAdapterService.executeColumnRule(columns));
        tableMetadata.setPrimaryMetas(getTablePrimaryColumn(columns));
        tableMetadata.setTableHash(calcTableHash(columns));
    }

    private List<String> getPrimaryColumnNames(List<PrimaryColumnBean> primaryColumnBeans) {
        return primaryColumnBeans.stream()
            .map(PrimaryColumnBean::getColumnName)
            .map(String::toLowerCase)
            .distinct()
            .collect(Collectors.toList());
    }

    private List<ColumnsMetaData> getTablePrimaryColumn(List<ColumnsMetaData> columnsMetaData) {
        return columnsMetaData.stream()
            .filter(meta -> ColumnKey.PRI.equals(meta.getColumnKey()))
            .sorted(Comparator.comparing(ColumnsMetaData::getOrdinalPosition))
            .collect(Collectors.toList());
    }

    /**
     * query table columns
     *
     * @param tableName table
     * @return table columns
     */
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        final List<ColumnsMetaData> columns = dataAccessService.queryTableColumnsMetaData(tableName);
        return ruleAdapterService.executeColumnRule(columns);
    }

    /**
     * Compare whether the source and destination table structures are the same.
     * <p>
     * When comparing table structures, ignore the capitalization of field names.
     * But in metadata, the capitalization of column names cannot be modified,
     * otherwise there is a possibility that query columns do not exist.
     * This is because the database itself determines the column case recognition pattern.
     *
     * @param columnsMetas columnsMetas
     * @return column hash
     */
    private long calcTableHash(List<ColumnsMetaData> columnsMetas) {
        StringBuilder buffer = new StringBuilder();
        columnsMetas.sort(Comparator.comparing(ColumnsMetaData::getOrdinalPosition));
        columnsMetas.forEach(column -> buffer.append(column.getColumnName().toLowerCase(Locale.ENGLISH))
            .append(column.getOrdinalPosition()));
        return HASH_UTIL.hashBytes(buffer.toString());
    }

    /**
     * check current table whether in check table list
     *
     * @param table table
     * @return true | false
     */
    public boolean checkTableContains(String table) {
        List<String> filterTableList = ruleAdapterService.executeTableRule(List.of(table));
        if (CollectionUtils.isNotEmpty(filterTableList)) {
            TableMetadata tableMetadata = queryTableMetadata(table);
            if (Objects.isNull(tableMetadata)) {
                LogUtils.warn(log, "table [{}] did not queried by queryTableMetadata", table);
                return false;
            }
            return tableMetadata.hasPrimary();
        } else {
            LogUtils.warn(log, "table [{}] does not in checklist", table);
            return false;
        }
    }

    public void initDataSourceSqlMode2ConfigCache() {
        String sqlMode = dataAccessService.sqlMode();
        if (Objects.isNull(sqlMode)) {
            ConfigCache.put(ConfigConstants.SQL_MODE_FORCE_REFRESH, false);
        } else {
            String[] sqlModeArray = sqlMode.split(",");
            String newSqlMode = Arrays.stream(sqlModeArray)
                .filter(mode -> !mode.equalsIgnoreCase(ConfigConstants.SQL_MODE_NAME_PAD_CHAR_TO_FULL_LENGTH))
                .collect(Collectors.joining(","));
            boolean isPadCharFull = ConfigCache.getBooleanValue(ConfigConstants.SQL_MODE_PAD_CHAR_TO_FULL_LENGTH);
            if (isPadCharFull) {
                newSqlMode += ConfigConstants.SQL_MODE_NAME_PAD_CHAR_TO_FULL_LENGTH;
            }
            boolean isForceRefreshConnectionSqlMode = sqlMode.length() != newSqlMode.length();
            if (isForceRefreshConnectionSqlMode) {
                ConfigCache.put(ConfigConstants.SQL_MODE_VALUE_CACHE, newSqlMode);
            }
            ConfigCache.put(ConfigConstants.SQL_MODE_FORCE_REFRESH, isForceRefreshConnectionSqlMode);
        }
    }

    /**
     * checkDatabaseEmpty
     *
     * @return boolean
     */
    public boolean bdsCheckDatabaseNotEmpty() {
        return dataAccessService.dasCheckDatabaseNotEmpty();
    }
}
