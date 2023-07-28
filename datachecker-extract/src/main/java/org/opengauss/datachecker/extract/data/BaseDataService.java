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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.service.RuleAdapterService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
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
    @Resource
    private DataAccessService dataAccessService;
    @Resource
    private RuleAdapterService ruleAdapterService;
    private final List<String> tableNameList = new LinkedList<>();

    /**
     * load check table list
     */
    public void loadCheckingTables() {
        tableNameList.clear();
        tableNameList.addAll(filterByTableRules(dataAccessService.queryTableNameList()));
    }

    /**
     * query check table list , and use rule.table
     * filter no primary key tables,and filter table rule( black and write list)
     *
     * @return table name list
     */
    public List<String> queryTableNameList() {
        if (CollectionUtils.isEmpty(tableNameList)) {
            tableNameList.addAll(filterByTableRules(dataAccessService.queryTableNameList()));
        }
        return tableNameList;
    }

    /**
     * query check table metadata list, and use rule.table
     *
     * @return table name list
     */
    public List<TableMetadata> queryTableMetadataList() {
        List<TableMetadata> metadataList = dataAccessService.queryTableMetadataList();
        List<String> tableList = queryTableNameList();
        return metadataList.stream().filter(meta -> tableList.contains(meta.getTableName()))
                           .collect(Collectors.toList());
    }

    private List<String> filterByTableRules(List<String> tableNameList) {
        return ruleAdapterService.executeTableRule(tableNameList);
    }

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
        TableMetadata tableMetadata = dataAccessService.queryTableMetadata(tableName);
        updateTableColumnMetaData(tableMetadata);
        return tableMetadata;
    }

    public void updateTableColumnMetaData(TableMetadata tableMetadata) {
        String tableName = tableMetadata.getTableName();
        final List<ColumnsMetaData> columns = dataAccessService.queryTableColumnsMetaData(tableName);
        tableMetadata.setColumnsMetas(ruleAdapterService.executeColumnRule(columns));
        tableMetadata.setPrimaryMetas(getTablePrimaryColumn(columns));
    }

    private List<ColumnsMetaData> getTablePrimaryColumn(List<ColumnsMetaData> columnsMetaData) {
        return columnsMetaData.stream().filter(meta -> ColumnKey.PRI.equals(meta.getColumnKey()))
                              .sorted(Comparator.comparing(ColumnsMetaData::getOrdinalPosition))
                              .collect(Collectors.toList());
    }

    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        final List<ColumnsMetaData> columns = dataAccessService.queryTableColumnsMetaData(tableName);
        return ruleAdapterService.executeColumnRule(columns);
    }

    public boolean contains(String table) {
        return tableNameList.contains(table);
    }
}
