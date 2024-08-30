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

package org.opengauss.datachecker.check.modules.check;

import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.check.client.ExtractFeignClient;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * QueryRowDataWapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
public class QueryRowDataWapper {
    private static final int MAX_QUERY_PAGE_SIZE = 100;

    private final FeignClientService feignClient;

    /**
     * QueryRowDataWapper constructed function
     *
     * @param feignClient feignClient
     */
    public QueryRowDataWapper(FeignClientService feignClient) {
        this.feignClient = feignClient;
    }

    /**
     * Query incremental data
     *
     * @param endpoint endpoint
     * @param dataLog  dataLog
     * @return result
     */
    public List<RowDataHash> queryCheckRowData(Endpoint endpoint, SourceDataLog dataLog) {
        if (dataLog == null || CollectionUtils.isEmpty(dataLog.getCompositePrimaryValues())) {
            return new ArrayList<>();
        }
        ExtractFeignClient client = feignClient.getClient(endpoint);
        Assert.isTrue(Objects.nonNull(client), endpoint + " feign client is null");

        final List<String> compositeKeys = dataLog.getCompositePrimaryValues();
        List<RowDataHash> result = new ArrayList<>();
        if (compositeKeys.size() > MAX_QUERY_PAGE_SIZE) {
            AtomicInteger cnt = new AtomicInteger(0);
            List<String> tempCompositeKeys = new ArrayList<>();
            compositeKeys.forEach(key -> {
                tempCompositeKeys.add(key);
                if (cnt.incrementAndGet() % MAX_QUERY_PAGE_SIZE == 0) {
                    SourceDataLog pageDataLog = getPageDataLog(dataLog, tempCompositeKeys);
                    if (Endpoint.SOURCE.equals(endpoint)) {
                        result.addAll(client.querySourceCheckRowData(pageDataLog).getData());
                    } else {
                        result.addAll(client.querySinkCheckRowData(pageDataLog).getData());
                    }
                    tempCompositeKeys.clear();
                }
            });
            if (CollectionUtils.isNotEmpty(tempCompositeKeys)) {
                SourceDataLog pageDataLog = getPageDataLog(dataLog, tempCompositeKeys);
                if (Endpoint.SOURCE.equals(endpoint)) {
                    result.addAll(client.querySourceCheckRowData(pageDataLog).getData());
                } else {
                    result.addAll(client.querySinkCheckRowData(pageDataLog).getData());
                }
                tempCompositeKeys.clear();
            }
        } else {
            if (Endpoint.SOURCE.equals(endpoint)) {
                result.addAll(client.querySourceCheckRowData(dataLog).getData());
            } else {
                result.addAll(client.querySinkCheckRowData(dataLog).getData());
            }
        }
        return result;
    }

    /**
     * Query incremental data
     *
     * @param endpoint endpoint
     * @param dataLog  dataLog
     * @return result
     */
    public List<RowDataHash> querySecondaryCheckRowData(Endpoint endpoint, SourceDataLog dataLog) {
        if (dataLog == null || CollectionUtils.isEmpty(dataLog.getCompositePrimaryValues())) {
            return new ArrayList<>();
        }
        ExtractFeignClient client = feignClient.getClient(endpoint);
        Assert.isTrue(Objects.nonNull(client), endpoint + " feign client is null");

        final List<String> compositeKeys = dataLog.getCompositePrimaryValues();
        List<RowDataHash> result = new ArrayList<>();
        if (compositeKeys.size() > MAX_QUERY_PAGE_SIZE) {
            AtomicInteger cnt = new AtomicInteger(0);
            List<String> tempCompositeKeys = new ArrayList<>();
            compositeKeys.forEach(key -> {
                tempCompositeKeys.add(key);
                if (cnt.incrementAndGet() % MAX_QUERY_PAGE_SIZE == 0) {
                    SourceDataLog pageDataLog = getPageDataLog(dataLog, tempCompositeKeys);
                    if (Endpoint.SOURCE.equals(endpoint)) {
                        result.addAll(client.querySourceSecondaryCheckRowData(pageDataLog).getData());
                    } else {
                        result.addAll(client.querySinkSecondaryCheckRowData(pageDataLog).getData());
                    }
                    tempCompositeKeys.clear();
                }
            });
            if (CollectionUtils.isNotEmpty(tempCompositeKeys)) {
                SourceDataLog pageDataLog = getPageDataLog(dataLog, tempCompositeKeys);
                if (Endpoint.SOURCE.equals(endpoint)) {
                    result.addAll(client.querySourceSecondaryCheckRowData(pageDataLog).getData());
                } else {
                    result.addAll(client.querySinkSecondaryCheckRowData(pageDataLog).getData());
                }
                tempCompositeKeys.clear();
            }
        } else {
            if (Endpoint.SOURCE.equals(endpoint)) {
                result.addAll(client.querySourceSecondaryCheckRowData(dataLog).getData());
            } else {
                result.addAll(client.querySinkSecondaryCheckRowData(dataLog).getData());
            }
        }
        return result;
    }

    private SourceDataLog getPageDataLog(SourceDataLog dataLog, List<String> tempCompositeKeys) {
        SourceDataLog pageDataLog = new SourceDataLog();
        pageDataLog.setTableName(dataLog.getTableName());
        pageDataLog.setCompositePrimarys(dataLog.getCompositePrimarys());
        if (Objects.nonNull(tempCompositeKeys)) {
            pageDataLog.setCompositePrimaryValues(tempCompositeKeys);
        } else {
            pageDataLog.setCompositePrimaryValues(dataLog.getCompositePrimaryValues());
        }
        return pageDataLog;
    }
}
