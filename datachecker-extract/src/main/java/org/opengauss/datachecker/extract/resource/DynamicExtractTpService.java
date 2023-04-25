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

package org.opengauss.datachecker.extract.resource;

import org.opengauss.datachecker.common.service.DynamicThreadPool;
import org.opengauss.datachecker.common.thread.ThreadPoolFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.EXTRACT_EXECUTOR;
import static org.opengauss.datachecker.common.constant.DynamicTpConstant.TABLE_PARALLEL_EXECUTOR;

/**
 * DynamicExtractTpService
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/24
 * @since ：11
 */
@Component
public class DynamicExtractTpService implements DynamicThreadPool {
    @Override
    public void buildDynamicThreadPoolExecutor(Map<String, ThreadPoolExecutor> executors, int corePoolSize,
        int maximumPoolSize) {
        executors.put(EXTRACT_EXECUTOR, buildExtractExecutor(corePoolSize, maximumPoolSize));
        executors.put(TABLE_PARALLEL_EXECUTOR, buildTableParallelExecutor(corePoolSize, maximumPoolSize));
    }

    private ThreadPoolExecutor buildTableParallelExecutor(int corePoolSize, int maximumPoolSize) {
        return ThreadPoolFactory
            .newThreadPool(TABLE_PARALLEL_EXECUTOR, corePoolSize, maximumPoolSize, Integer.MAX_VALUE);
    }

    private ThreadPoolExecutor buildExtractExecutor(int corePoolSize, int maximumPoolSize) {
        return ThreadPoolFactory.newThreadPool(EXTRACT_EXECUTOR, corePoolSize, maximumPoolSize, Integer.MAX_VALUE);
    }
}
