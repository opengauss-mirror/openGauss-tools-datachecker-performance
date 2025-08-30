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

package org.opengauss.datachecker.check.load;

import org.opengauss.datachecker.check.service.EndpointManagerService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;

/**
 * HeartBeatStartLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Order(1)
@Service
public class HeartBeatStartLoader extends AbstractCheckLoader {
    @Resource
    private EndpointManagerService endpointManagerService;
    @Resource
    private HeartBeatKeeper heartBeatKeeper;
    private int retryTimes = 0;

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        Boolean enabled = ConfigCache.getBooleanValue(ConfigConstants.ENABLE_HEART_BEAT_HEATH);
        if (!enabled) {
            return;
        }
        endpointManagerService.heartBeat();
        boolean isSourceHealth;
        boolean isSinkHealth;
        while (!endpointManagerService.isEndpointHealth()) {
            isSourceHealth = endpointManagerService.checkEndpointHealth(Endpoint.SOURCE);
            isSinkHealth = endpointManagerService.checkEndpointHealth(Endpoint.SINK);
            LogUtils.warn(log, "endpoint source={},sink={} does not health, please wait a moment!", isSourceHealth,
                isSinkHealth);
            ThreadUtil.sleep(retryIntervalTimes);
            retryTimes++;
            if (retryTimes >= maxRetryTimes) {
                shutdown("heart beat retry too many times");
            }
        }
        isSourceHealth = endpointManagerService.checkEndpointHealth(Endpoint.SOURCE);
        isSinkHealth = endpointManagerService.checkEndpointHealth(Endpoint.SINK);
        LogUtils.info(log, "endpoint source={},sink={} health ", isSourceHealth, isSinkHealth);
        heartBeatKeeper.backendKeeper();
    }
}
