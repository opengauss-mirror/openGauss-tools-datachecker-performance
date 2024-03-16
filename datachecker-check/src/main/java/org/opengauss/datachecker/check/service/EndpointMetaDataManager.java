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

package org.opengauss.datachecker.check.service;

import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.PageExtract;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * EndpointMetaDataManager
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/24
 * @since ：11
 */
@Service
@RequiredArgsConstructor
public class EndpointMetaDataManager {
    private static final Logger log = LogUtils.getLogger(EndpointMetaDataManager.class);
    private static final List<String> CHECK_TABLE_LIST = new ArrayList<>();
    private static final List<String> MISS_TABLE_LIST = new ArrayList<>();
    private static final Map<String, TableMetadata> SOURCE_METADATA = new HashMap<>();
    private static final Map<String, TableMetadata> SINK_METADATA = new HashMap<>();
    private static final Map<Endpoint, Integer> REAL_TABLE_COUNT = new EnumMap<>(Endpoint.class);

    private Map<Endpoint, Integer> loadMetadataCompleted = new EnumMap<>(Endpoint.class);
    private final EndpointStatusManager endpointStatusManager;
    private final FeignClientService feignClientService;

    /**
     * Reload metadata information
     */
    public void load() {
        if (MapUtils.isNotEmpty(SOURCE_METADATA) && MapUtils.isNotEmpty(SINK_METADATA)) {
            final List<String> sourceTables = getEndpointTableNamesSortByTableRows(SOURCE_METADATA);
            final List<String> sinkTables = getEndpointTableNamesSortByTableRows(SINK_METADATA);
            REAL_TABLE_COUNT.put(Endpoint.SOURCE, CollectionUtils.isNotEmpty(sourceTables) ? sourceTables.size() : 0);
            REAL_TABLE_COUNT.put(Endpoint.SINK, CollectionUtils.isNotEmpty(sinkTables) ? sinkTables.size() : 0);
            LogUtils.info(log, "loaded meatdata table list finished");
            final List<String> checkTables = compareAndFilterEndpointTables(sourceTables, sinkTables);
            LogUtils.info(log, "filter check meatdata table list finished");
            final List<String> missTables = compareAndFilterMissTables(sourceTables, sinkTables);
            LogUtils.info(log, "filter miss meatdata table list finished");
            if (CollectionUtils.isNotEmpty(checkTables)) {
                CHECK_TABLE_LIST.addAll(checkTables);
            }
            if (CollectionUtils.isNotEmpty(missTables)) {
                MISS_TABLE_LIST.addAll(missTables);
            }
        } else {
            LogUtils.warn(log, "the metadata information is empty, and the verification is terminated abnormally,"
                + "sourceMetadata={},sinkMetadata={}", SOURCE_METADATA.size(), SINK_METADATA.size());
        }
    }

    public Map<Endpoint, Integer> getRealTableCount() {
        return REAL_TABLE_COUNT;
    }

    /**
     * Query the metadata information of the source side and target side, and return the metadata query status
     *
     * @return metadata query status
     */
    public boolean isMetaLoading() {
        fetchMeatdataPage(Endpoint.SOURCE, SOURCE_METADATA);
        fetchMeatdataPage(Endpoint.SINK, SINK_METADATA);
        return !(loadMetadataCompleted.containsKey(Endpoint.SOURCE) && loadMetadataCompleted.containsKey(
            Endpoint.SINK));
    }

    private void fetchMeatdataPage(Endpoint endpoint, Map<String, TableMetadata> meatdataMap) {
        if (loadMetadataCompleted.containsKey(endpoint) && MapUtils.isEmpty(meatdataMap)) {
            LogUtils.debug(log, "loading {} metadata", endpoint);
            PageExtract pageExtract = feignClientService.getExtractMetaPageInfo(endpoint);
            while (pageExtract.getPageNo() < pageExtract.getPageCount()) {
                Map<String, TableMetadata> tableMetadataMap =
                    feignClientService.queryMetaDataOfSchema(endpoint, pageExtract);
                if (MapUtils.isNotEmpty(tableMetadataMap)) {
                    meatdataMap.putAll(tableMetadataMap);
                }
                LogUtils.info(log, "loaded {} meatdata page {}", endpoint, pageExtract);
                pageExtract.incrementAndGetPageNo();
            }
            LogUtils.debug(log, "loading {} metadata end ", endpoint);
        }
    }

    private List<String> compareAndFilterMissTables(List<String> sourceTables, List<String> sinkTables) {
        List<String> missList = new ArrayList<>();
        missList.addAll(diffList(sourceTables, sinkTables));
        missList.addAll(diffList(sinkTables, sourceTables));
        return missList;
    }

    private List<String> diffList(List<String> source, List<String> sink) {
        return source.stream()
                     .filter(table -> !sink.contains(table))
                     .collect(Collectors.toList());
    }

    /**
     * Get the table metadata information of the specified endpoint
     *
     * @param endpoint  endpoint
     * @param tableName tableName
     * @return metadata
     */
    public TableMetadata getTableMetadata(Endpoint endpoint, String tableName) {
        TableMetadata metadata = null;
        if (Objects.equals(Endpoint.SINK, endpoint)) {
            if (SINK_METADATA.containsKey(tableName)) {
                metadata = SINK_METADATA.get(tableName);
            }
        } else {
            if (SOURCE_METADATA.containsKey(tableName)) {
                metadata = SOURCE_METADATA.get(tableName);
            }
        }
        return metadata;
    }

    /**
     * Calculate and return the number of verification tasks
     *
     * @return the number of verification tasks
     */
    public int getCheckTaskCount() {
        return CHECK_TABLE_LIST.size() + MISS_TABLE_LIST.size();
    }

    public void clearCache() {
        CHECK_TABLE_LIST.clear();
        MISS_TABLE_LIST.clear();
        SOURCE_METADATA.clear();
        SINK_METADATA.clear();
    }

    private List<String> compareAndFilterEndpointTables(List<String> sourceTables, List<String> sinkTables) {
        return sourceTables.stream()
                           .filter(sinkTables::contains)
                           .collect(Collectors.toList());
    }

    private List<String> getEndpointTableNamesSortByTableRows(Map<String, TableMetadata> metadataMap) {
        return new ArrayList<>(metadataMap.keySet());
    }

    /**
     * View the health status of all endpoints
     *
     * @return health status
     */
    public boolean isEndpointHealth() {
        return endpointStatusManager.isEndpointHealth();
    }

    /**
     * check table list
     *
     * @return check table list
     */
    public List<String> getCheckTableList() {
        return CHECK_TABLE_LIST;
    }

    /**
     * miss table list
     *
     * @return miss table list
     */
    public List<String> getMissTableList() {
        return MISS_TABLE_LIST;
    }

    /**
     * query table metadata by jdbc
     *
     * @param endpoint  endpoint
     * @param tableName tableName
     * @return TableMetadata
     */
    public TableMetadata queryIncrementMetaData(Endpoint endpoint, String tableName) {
        return feignClientService.queryIncrementMetaData(endpoint, tableName);
    }

    /**
     * set endpoint is no table
     *
     * @param endpoint endpoint
     * @return boolean
     */
    public boolean isCheckTableEmpty(Endpoint endpoint) {
        return feignClientService.isCheckTableEmpty(endpoint, true);
    }

    /**
     * refresh endpoint load metadata completed
     *
     * @param endpoint endpoint
     */
    public void refreshLoadMetadataStatus(Endpoint endpoint) {
        LogUtils.debug(log, "refresh {} load metadata completed", endpoint);
        loadMetadataCompleted.put(endpoint, 1);
    }
}
