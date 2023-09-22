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
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.DispatchClientException;
import org.opengauss.datachecker.common.web.Result;

import java.util.ArrayList;
import java.util.List;

/**
 * QueryRowDataWapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
public class QueryRowDataWapper {
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
     * @param endpoint  endpoint
     * @param tableName tableName
     * @return result
     */
    public List<RowDataHash> queryIncrementRowData(Endpoint endpoint, String tableName) {
        List<RowDataHash> data = new ArrayList<>();
        Result<List<RowDataHash>> result = feignClient.getClient(endpoint).queryIncrementTopicData(tableName);
        if (!result.isSuccess()) {
            throw new DispatchClientException(endpoint,
                "query topic data of tableName " + tableName + " error, " + result.getMessage());
        }
        while (result.isSuccess() && !CollectionUtils.isEmpty(result.getData())) {
            data.addAll(result.getData());
            result = feignClient.getClient(endpoint).queryIncrementTopicData(tableName);
        }
        return data;
    }

    /**
     * Query incremental data
     *
     * @param endpoint endpoint
     * @param dataLog  dataLog
     * @return result
     */
    public List<RowDataHash> queryRowData(Endpoint endpoint, SourceDataLog dataLog) {
        if (dataLog == null || CollectionUtils.isEmpty(dataLog.getCompositePrimaryValues())) {
            return new ArrayList<>();
        }
        Result<List<RowDataHash>> result = feignClient.getClient(endpoint).querySecondaryCheckRowData(dataLog);
        if (!result.isSuccess()) {
            throw new DispatchClientException(endpoint,
                "query topic data of tableName " + dataLog.getTableName() + " error, " + result.getMessage());
        }
        return result.getData();
    }
}
