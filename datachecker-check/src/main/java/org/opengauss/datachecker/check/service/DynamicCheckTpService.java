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

import org.opengauss.datachecker.common.service.DynamicThreadPool;
import org.opengauss.datachecker.common.thread.ThreadPoolFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.CHECK_EXECUTOR;

/**
 * DynamicCheckTpService
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/24
 * @since ：11
 */
@Component
public class DynamicCheckTpService implements DynamicThreadPool {
    @Override
    public void buildDynamicThreadPoolExecutor(Map<String, ThreadPoolExecutor> executors, int corePoolSize,
        int maximumPoolSize) {
        executors.put(CHECK_EXECUTOR, buildCheckExecutor(corePoolSize, maximumPoolSize));
    }

    private ThreadPoolExecutor buildCheckExecutor(int corePoolSize, int maximumPoolSize) {
        return ThreadPoolFactory.newThreadPool(CHECK_EXECUTOR, corePoolSize, maximumPoolSize, Integer.MAX_VALUE);
    }
}
