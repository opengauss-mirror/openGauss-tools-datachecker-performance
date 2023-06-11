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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.CheckMetaDataException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
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
@Slf4j
@Service
@RequiredArgsConstructor
public class EndpointMetaDataManager {
    private static final List<String> CHECK_TABLE_LIST = new ArrayList<>();
    private static final List<String> MISS_TABLE_LIST = new ArrayList<>();
    private static final Map<String, TableMetadata> SOURCE_METADATA = new HashMap<>();
    private static final Map<String, TableMetadata> SINK_METADATA = new HashMap<>();

    private final EndpointStatusManager endpointStatusManager;
    private final FeignClientService feignClientService;

    /**
     * Reload metadata information
     */
    public void load() {
        if (MapUtils.isNotEmpty(SOURCE_METADATA) && MapUtils.isNotEmpty(SINK_METADATA)) {
            final List<String> sourceTables = getEndpointTableNamesSortByTableRows(SOURCE_METADATA);
            final List<String> sinkTables = getEndpointTableNamesSortByTableRows(SINK_METADATA);
            final List<String> checkTables = compareAndFilterEndpointTables(sourceTables, sinkTables);
            final List<String> missTables = compareAndFilterMissTables(sourceTables, sinkTables);
            CHECK_TABLE_LIST.addAll(checkTables);
            MISS_TABLE_LIST.addAll(missTables);
        } else {
            log.error("the metadata information is empty, and the verification is terminated abnormally,"
                + "sourceMetadata={},sinkMetadata={}", SOURCE_METADATA.size(), SINK_METADATA.size());
            throw new CheckMetaDataException(
                "the metadata information is empty, and the verification is terminated abnormally");
        }
    }

    /**
     * Query the metadata information of the source side and target side, and return the metadata query status
     *
     * @return metadata query status
     */
    public boolean isMetaLoading() {
        if (MapUtils.isEmpty(SOURCE_METADATA)) {
            log.debug("loading {} metadata", Endpoint.SOURCE);
            SOURCE_METADATA.putAll(feignClientService.queryMetaDataOfSchema(Endpoint.SOURCE));
            log.debug("loading {} metadata end ", Endpoint.SOURCE);
        }
        if (MapUtils.isEmpty(SINK_METADATA)) {
            log.debug("loading {} metadata", Endpoint.SINK);
            SINK_METADATA.putAll(feignClientService.queryMetaDataOfSchema(Endpoint.SINK));
            log.debug("loading {} metadata end ", Endpoint.SINK);
        }
        return SOURCE_METADATA.isEmpty() || SINK_METADATA.isEmpty();
    }

    private List<String> compareAndFilterMissTables(List<String> sourceTables, List<String> sinkTables) {
        List<String> missList = new ArrayList<>();
        missList.addAll(diffList(sourceTables, sinkTables));
        missList.addAll(diffList(sinkTables, sourceTables));
        return missList;
    }

    private List<String> diffList(List<String> source, List<String> sink) {
        return source.stream().filter(table -> !sink.contains(table)).collect(Collectors.toList());
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
        return sourceTables.stream().filter(sinkTables::contains).collect(Collectors.toList());
    }

    private List<String> getEndpointTableNamesSortByTableRows(Map<String, TableMetadata> metadataMap) {
        return metadataMap.values().stream().sorted(Comparator.comparing(TableMetadata::getTableRows))
                          .map(TableMetadata::getTableName).collect(Collectors.toUnmodifiableList());
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
}
