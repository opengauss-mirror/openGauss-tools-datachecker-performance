/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.exception.CommonException;
import org.opengauss.datachecker.extract.service.thread.ExtractMockDataThread;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * ExtractMockDataService
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/26
 * @since ：11
 */
@Slf4j
@Service
public class ExtractMockDataService {

    private static final int MAX_THREAD_COUNT = 100;

    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;
    @Autowired
    private DataSource dataSourceOne;

    /**
     * batchMockData
     *
     * @param tableName   tableName
     * @param totalCount  totalCount
     * @param threadCount threadCount
     */
    public void batchMockData(String tableName, long totalCount, int threadCount) {
        try {
            Assert.isTrue(threadCount < MAX_THREAD_COUNT,
                "The total number of threads set cannot exceed the maximum total number of threads");
            long batchCount = totalCount / threadCount;
            log.info("plan batch insert thread, tableName = {}, threadCount = {} ,totalCount = {} , batchCount = {}",
                tableName, threadCount, totalCount, batchCount);
            List<Future> mockFutureList = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                if (i == (threadCount - 1)) {
                    batchCount = batchCount + totalCount % threadCount;
                }
                mockFutureList.add(threadPoolTaskExecutor
                    .submit(new ExtractMockDataThread(dataSourceOne, tableName, batchCount, i + 1)));
            }
            mockFutureList.forEach(future -> {
                while (true) {
                    if (future.isDone() && !future.isCancelled()) {
                        break;
                    }
                }
            });
        } catch (CommonException ex) {
            log.error("batchMockData", ex.getMessage());
        }
    }
}
