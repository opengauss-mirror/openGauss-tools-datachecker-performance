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


import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Objects;
/**
 * CheckPointRegister
 *
 * @author ：lvlintao
 * @date ：Created in 2023/10/28
 * @since ：11
 */
@Component
public class CheckPointRegister {
    private static final Logger log = LogUtils.getLogger();
    private CheckPointSwapRegister checkPointSwapRegister;
    @Resource
    private KafkaServiceManager kafkaServiceManager;

    /**
     * start table checkPoint monitor
     */
    public synchronized void startMonitor() {
        if (Objects.isNull(checkPointSwapRegister)) {
            kafkaServiceManager.initAdminClient();
            checkPointSwapRegister = new CheckPointSwapRegister(kafkaServiceManager);
            checkPointSwapRegister.create(ConfigCache.getValue(ConfigConstants.PROCESS_NO));
            checkPointSwapRegister.pollSwapPoint();
            checkPointSwapRegister.registerCheckPoint();
        }
    }

    /**
     * stop the table checkPoint monitor
     *
     * @param endpoint endpoint
     */
    public void stopMonitor(Endpoint endpoint) {
        checkPointSwapRegister.stopMonitor(endpoint);
    }
}
