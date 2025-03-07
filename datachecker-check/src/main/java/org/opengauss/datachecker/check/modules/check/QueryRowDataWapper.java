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

import cn.hutool.core.thread.ThreadUtil;

import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.check.client.ExtractFeignClient;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * QueryRowDataWapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
public class QueryRowDataWapper {
    private static final int MAX_WAIT_TIMES = 100;

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
     * @param dataLog dataLog
     * @return result
     */
    public List<RowDataHash> queryCheckRowData(Endpoint endpoint, SourceDataLog dataLog) {
        if (dataLog == null || CollectionUtils.isEmpty(dataLog.getCompositePrimaryValues())) {
            return new ArrayList<>();
        }
        ExtractFeignClient client = feignClient.getClient(endpoint);
        Assert.isTrue(Objects.nonNull(client), endpoint + " feign client is null");
        String queryId = client.queryCheckRowDataAsync(dataLog).getData();
        int waitTimes = 0;
        while (!client.queryCheckRowDataAsyncStatus(queryId).getData() && waitTimes < MAX_WAIT_TIMES) {
            ThreadUtil.safeSleep(1000);
            waitTimes++;
        }
        if (waitTimes >= MAX_WAIT_TIMES) {
            throw new CheckingException("async query check row data wait timeout | queryId:" + queryId);
        }
        Result<List<RowDataHash>> listResult = client.queryCheckRowDataAsyncData(queryId);
        return listResult.getData();
    }
}
