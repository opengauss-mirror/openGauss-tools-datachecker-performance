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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;

/**
 * @author ：wangchao
 * @date ：Created in 2023/8/9
 * @since ：11
 */
public class MemoryOperations {
    private static final Logger log = LogUtils.getLogger();
    private static final int LOG_WAIT_TIMES = 600;
    private final ResourceManager resourceManager;

    /**
     * constructor
     *
     * @param resourceManager resourceManager
     */
    public MemoryOperations(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
    }

    public synchronized void takeMemory(long free) {
        int waitTimes = 0;
        while (!canExecQuery(free)) {
            if (isShutdown()) {
                break;
            }
            ThreadUtil.sleepOneSecond();
            waitTimes++;
            if (waitTimes >= LOG_WAIT_TIMES) {
                log.warn("wait times , try to take connection");
                waitTimes = 0;
            }
        }
    }

    public synchronized void release() {
        resourceManager.release();
    }

    private boolean canExecQuery(long free) {
        return free > 0 ? resourceManager.canExecQuery(free) : resourceManager.canExecQuery();
    }

    private boolean isShutdown() {
        return resourceManager.isShutdown();
    }
}
