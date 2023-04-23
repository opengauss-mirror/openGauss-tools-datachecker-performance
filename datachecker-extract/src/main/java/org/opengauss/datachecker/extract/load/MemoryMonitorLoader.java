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

package org.opengauss.datachecker.extract.load;

import org.opengauss.datachecker.common.service.MemoryManagerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * MemoryMonitorLoader
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/30
 * @since ：11
 */
@Order(200)
@Service
public class MemoryMonitorLoader extends AbstractExtractLoader {
    @Resource
    private MemoryManagerService memoryManagerService;
    @Value("${spring.memory-monitor-enable}")
    private boolean isEnableMemoryMonitor;

    @Override
    public void load(ExtractEnvironment extractEnvironment) {
        memoryManagerService.startMemoryManager(isEnableMemoryMonitor);
    }
}
