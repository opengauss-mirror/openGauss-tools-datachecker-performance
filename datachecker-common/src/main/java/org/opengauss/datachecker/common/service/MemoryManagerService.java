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

package org.opengauss.datachecker.common.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MemoryManagerService
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
@Service
public class MemoryManagerService {
    private AtomicBoolean isServerStarted = new AtomicBoolean(false);
    private AtomicBoolean isEnableMemoryMonitored = new AtomicBoolean(false);

    /**
     * memory monitor schedule
     */
    @Scheduled(cron = "0/2 * * * * ?")
    public void memory() {
        if (isEnableMemoryMonitored.get() && isServerStarted.get()) {
            MemoryManager.getRuntimeInfo();
        }
    }

    /**
     * start memory schedule
     *
     * @param isEnableMemoryMonitor isEnableMemoryMonitor
     */
    public void startMemoryManager(boolean isEnableMemoryMonitor) {
        isServerStarted.set(true);
        isEnableMemoryMonitored.set(isEnableMemoryMonitor);
    }
}
