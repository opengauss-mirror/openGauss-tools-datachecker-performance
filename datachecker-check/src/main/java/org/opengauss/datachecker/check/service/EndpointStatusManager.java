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

package org.opengauss.datachecker.check.service;

import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Data extraction endpoint state management
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/26
 * @since ：11
 */
@Service
public class EndpointStatusManager {
    private static final Pair<Boolean, Boolean> STATUS = Pair.of(false, false);
    private static Lock LOCK = new ReentrantLock();

    /**
     * Reset the health state of the endpoint
     *
     * @param endpoint endpoint {@value Endpoint#API_DESCRIPTION}
     * @param isHealth endpoint health status
     */
    public void resetStatus(Endpoint endpoint, boolean isHealth) {
        LOCK.lock();
        try {
            if (Objects.equals(endpoint, Endpoint.SOURCE)) {
                Pair.of(isHealth, STATUS);
            } else {
                Pair.of(STATUS, isHealth);
            }
        } finally {
            LOCK.unlock();
        }
    }

    private void reset() {
        Pair.of(false, STATUS);
        Pair.of(STATUS, false);
    }

    /**
     * View the health status of all endpoints
     *
     * @return health status
     */
    public boolean isEndpointHealth() {
        return Objects.equals(STATUS.getSink(), Boolean.TRUE) && Objects.equals(STATUS.getSource(), Boolean.TRUE);
    }

    public boolean getHealthStatus(Endpoint endpoint) {
        return Objects.equals(Endpoint.SOURCE, endpoint) ? STATUS.getSource() : STATUS.getSink();
    }
}
