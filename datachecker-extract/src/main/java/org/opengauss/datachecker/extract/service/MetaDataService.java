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

package org.opengauss.datachecker.extract.service;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.thread.ThreadPoolFactory;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * MetaDataService
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Service
public class MetaDataService {
    private static final Logger log = LogUtils.getLogger();
    @Resource
    private BaseDataService baseDataService;
    @Resource
    private ResourceManager resourceManager;
    private volatile AtomicBoolean isCheckTableEmpty = new AtomicBoolean(false);
    private volatile MetadataLoadProcess metadataLoadProcess = new MetadataLoadProcess();

    /**
     * Return database metadata information through cache
     *
     * @return metadata information
     */
    public Map<String, TableMetadata> queryMetaDataOfSchemaCache() {
        log.debug("load table metadata from cache isEmpty=[{}]", MetaDataCache.isEmpty());
        return MetaDataCache.getAll();
    }

    public List<String> queryAllTableNames() {
        return baseDataService.queryTableNameList();
    }

    /**
     * Asynchronous loading of metadata cache information
     */
    public void loadMetaDataOfSchemaCache() {
        if (MetaDataCache.isEmpty()) {
            Map<String, TableMetadata> metaDataMap = queryMetaDataOfSchema();
            MetaDataCache.putMap(metaDataMap);
            log.info("put table metadata in cache [{}]", metaDataMap.size());
        }
    }

    /**
     * Return metadata loading progress
     *
     * @return metadata loading progress
     */
    public MetadataLoadProcess getMetadataLoadProcess() {
        return metadataLoadProcess;
    }

    public Map<String, TableMetadata> queryMetaDataOfSchema() {
        Map<String, TableMetadata> tableMetadataMap = new ConcurrentHashMap<>();
        final List<TableMetadata> tableMetadataList = baseDataService.queryTableMetadataList();
        log.info("query table metadata {}", tableMetadataList.size());
        if (CollectionUtils.isEmpty(tableMetadataList)) {
            return tableMetadataMap;
        }
        List<Future<?>> futures = new LinkedList<>();
        int maxConnection = resourceManager.maxConnectionCount();
        ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, maxConnection / 2));
        metadataLoadProcess.setTotal(tableMetadataList.size());
        tableMetadataList.forEach(tableMetadata -> {
            Future<?> future = executor.submit(() -> {
                takeConnection();
                if (resourceManager.isShutdown()) {
                    log.warn("extract service is shutdown ,task set table metadata of table is canceled!");
                } else {
                    baseDataService.updateTableColumnMetaData(tableMetadata);
                    tableMetadataMap.put(tableMetadata.getTableName(), tableMetadata);
                }
                log.debug("load table and its columns {}  hasPrimary={}", tableMetadata.getTableName(),
                    tableMetadata.hasPrimary());
                resourceManager.release();
            });
            futures.add(future);
        });
        futures.forEach(future -> {
            try {
                future.get();
                metadataLoadProcess.setLoadCount(metadataLoadProcess.getLoadCount() + 1);
            } catch (InterruptedException | ExecutionException exp) {
                log.warn("extract table column failed with exp: {}", exp);
            }
        });
        executor.shutdown();
        metadataLoadProcess.setLoadCount(metadataLoadProcess.getTotal());
        log.info("query table column metadata {}", tableMetadataList.size());
        Map<String, TableMetadata> filterNoPrimary;
        filterNoPrimary = tableMetadataMap.entrySet()
                                          .stream()
                                          .filter(entry -> CollectionUtils.isNotEmpty(entry.getValue()
                                                                                           .getPrimaryMetas()))
                                          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        log.info("filter table which does not have primary metadata {}", filterNoPrimary.size());
        tableMetadataMap.clear();
        tableMetadataMap.putAll(filterNoPrimary);
        baseDataService.matchRowRules(tableMetadataMap);
        log.info("build table metadata [{}]", tableMetadataMap.size());
        return tableMetadataMap;
    }

    private void takeConnection() {
        while (!resourceManager.canExecQuery()) {
            if (resourceManager.isShutdown()) {
                break;
            }
            log.info("jdbc connection resource is not enough");
            ThreadUtil.sleep(50);
        }
    }

    public TableMetadata getMetaDataOfSchemaByCache(String tableName) {
        if (!MetaDataCache.containsKey(tableName)) {
            final TableMetadata tableMetadata = baseDataService.queryTableMetadata(tableName);
            if (Objects.nonNull(tableMetadata)) {
                MetaDataCache.put(tableName, tableMetadata);
            }
        }
        return MetaDataCache.get(tableName);
    }

    /**
     * query column Metadata info
     *
     * @param tableName tableName
     * @return column Metadata info
     */
    public List<ColumnsMetaData> queryTableColumnMetaDataOfSchema(String tableName) {
        return baseDataService.queryTableColumnsMetaData(tableName);
    }

    private List<ColumnsMetaData> getTablePrimaryColumn(List<ColumnsMetaData> columnsMetaData) {
        return columnsMetaData.stream()
                              .filter(meta -> ColumnKey.PRI.equals(meta.getColumnKey()))
                              .sorted(Comparator.comparing(ColumnsMetaData::getOrdinalPosition))
                              .collect(Collectors.toList());
    }

    /**
     * queryIncrementMetaData
     *
     * @param tableName tableName
     * @return TableMetadata
     */
    public TableMetadata queryIncrementMetaData(String tableName) {
        return baseDataService.queryTableMetadata(tableName);
    }

    /**
     * updateTableMetadata
     *
     * @param table         table
     * @param tableMetadata tableMetadata
     */
    public void updateTableMetadata(String table, TableMetadata tableMetadata) {
        if (Objects.isNull(tableMetadata)) {
            MetaDataCache.remove(table);
        } else {
            MetaDataCache.put(tableMetadata.getTableName(), tableMetadata);
        }
    }

    public synchronized Boolean isCheckTableEmpty(boolean isForced) {
        if (isForced) {
            isCheckTableEmpty.set(CollectionUtils.isEmpty(queryAllTableNames()));
            log.info("check database table (query) is {}", isCheckTableEmpty.get() ? "empty" : " not empty");
        }
        log.info("check database table is {}", isCheckTableEmpty.get() ? "empty" : " not empty");
        return isCheckTableEmpty.get();
    }
}
