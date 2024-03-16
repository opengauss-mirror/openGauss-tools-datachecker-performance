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
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.entry.extract.PageExtract;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
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
    private static final Logger log = LogUtils.getLogger(MetaDataService.class);

    @Resource
    private BaseDataService baseDataService;
    @Resource
    private ResourceManager resourceManager;
    private AtomicBoolean isCheckTableEmpty = new AtomicBoolean(false);
    private MetadataLoadProcess metadataLoadProcess = new MetadataLoadProcess();
    private List<String> metaKeyList = new ArrayList<>();

    /**
     * Return database metadata information through cache
     *
     * @param pageExtract pageExtract
     * @return metadata information
     */
    public Map<String, TableMetadata> queryMetaDataOfSchemaCache(PageExtract pageExtract) {
        LogUtils.debug(log, "load table metadata from cache isEmpty=[{}]", MetaDataCache.isEmpty());
        int pageStartIdx = pageExtract.getPageStartIdx();
        int pageEndIdx = pageExtract.getPageEndIdx();
        Map<String, TableMetadata> map = new HashMap<>();
        for (; pageStartIdx < pageEndIdx; pageStartIdx++) {
            String key = metaKeyList.get(pageStartIdx);
            map.put(key, MetaDataCache.get(key));
        }
        return map;
    }

    /**
     * 获取抽取元数据分页信息
     *
     * @return 分页信息
     */
    public PageExtract getExtractMetaPageInfo() {
        metaKeyList.addAll(Objects.requireNonNull(MetaDataCache.getAllKeys()));
        return PageExtract.buildInitPage(metaKeyList.size());
    }

    public List<String> queryAllTableNames() {
        return baseDataService.bdsQueryTableNameList();
    }

    /**
     * Asynchronous loading of metadata cache information
     */
    public void loadMetaDataOfSchemaCache() {
        if (MetaDataCache.isEmpty()) {
            Map<String, TableMetadata> metaDataMap = mdsQueryMetaDataOfSchema();
            MetaDataCache.putMap(metaDataMap);
            LogUtils.info(log, "put table metadata in cache [{}]", metaDataMap.size());
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

    public Map<String, TableMetadata> mdsQueryMetaDataOfSchema() {
        Map<String, TableMetadata> tableMetadataMap = new ConcurrentHashMap<>();
        final List<TableMetadata> tableMetadataList = baseDataService.bdsQueryTableMetadataList();
        LogUtils.info(log, "query table metadata {}", tableMetadataList.size());
        if (CollectionUtils.isEmpty(tableMetadataList)) {
            return tableMetadataMap;
        }
        Map<String, List<PrimaryColumnBean>> tablePrimaryColumns = baseDataService.queryTablePrimaryColumns();
        List<Future<?>> futures = new LinkedList<>();
        int initConnection = ConfigCache.getIntValue(ConfigConstants.DRUID_INITIAL_SIZE);
        ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, initConnection / 2));
        metadataLoadProcess.setTotal(tableMetadataList.size());
        tableMetadataList.forEach(tableMetadata -> {
            Future<?> future = executor.submit(() -> {
                takeConnection();
                if (resourceManager.isShutdown()) {
                    LogUtils.warn(log, "extract service is shutdown ,task set table metadata of table is canceled!");
                } else {
                    List<PrimaryColumnBean> primaryColumnList = tablePrimaryColumns.get(tableMetadata.getTableName());
                    baseDataService.updateTableColumnMetaData(tableMetadata, primaryColumnList);
                    tableMetadataMap.put(tableMetadata.getTableName(), tableMetadata);
                }
                LogUtils.debug(log, "load table and its columns {}  hasPrimary={}", tableMetadata.getTableName(),
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
                LogUtils.warn(log, "extract table column failed with exp:", exp);
            }
        });
        executor.shutdown();
        metadataLoadProcess.setLoadCount(metadataLoadProcess.getTotal());
        LogUtils.debug(log, "query table column metadata {}", tableMetadataList.size());
        Map<String, TableMetadata> filterNoPrimary;
        filterNoPrimary = tableMetadataMap.entrySet()
                                          .stream()
                                          .filter(entry -> CollectionUtils.isNotEmpty(entry.getValue()
                                                                                           .getPrimaryMetas()))
                                          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        LogUtils.debug(log, "filter table which does not have primary metadata {}", filterNoPrimary.size());
        tableMetadataMap.clear();
        tableMetadataMap.putAll(filterNoPrimary);
        baseDataService.matchRowRules(tableMetadataMap);
        LogUtils.info(log, "build table metadata [{}]", tableMetadataMap.size());
        return tableMetadataMap;
    }

    private void takeConnection() {
        while (!resourceManager.canExecQuery()) {
            if (resourceManager.isShutdown()) {
                break;
            }
            LogUtils.warn(log, "jdbc connection resource is not enough");
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

    /**
     * mdsIsCheckTableEmpty
     *
     * @param isForced isForced
     * @return boolean
     */
    public synchronized boolean mdsIsCheckTableEmpty(boolean isForced) {
        if (isForced) {
            isCheckTableEmpty.set(!baseDataService.bdsCheckDatabaseNotEmpty());
            LogUtils.info(log, "check database table (query) is {}", isCheckTableEmpty.get() ? "empty" : " not empty");
        } else {
            LogUtils.info(log, "check database table is {}", isCheckTableEmpty.get() ? "empty" : " not empty");
        }
        return isCheckTableEmpty.get();
    }
}
