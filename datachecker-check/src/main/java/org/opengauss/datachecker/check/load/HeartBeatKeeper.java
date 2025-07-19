/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.load;

import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.service.EndpointManagerService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * HeartBeatStartLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Service
public class HeartBeatKeeper {
    private static final String ERROR_EXIT_MESSAGE = "heart beat checked endpoint process is not health";

    @Resource
    private ShutdownService shutdownService;
    @Resource
    private FeignClientService feignClientService;
    @Resource
    private EndpointManagerService endpointManagerService;

    /**
     * Check the health status of the backend
     */
    @Async
    public void backendKeeper() {
        if (!ConfigCache.getBooleanValue(ConfigConstants.ENABLE_HEART_BEAT_HEATH)) {
            return;
        }
        while (endpointManagerService.isEndpointHealth()) {
            ThreadUtil.sleep(1000);
        }
        feignClientService.shutdown(ERROR_EXIT_MESSAGE);
        endpointManagerService.stopHeartBeat();
        shutdownService.shutdown(ERROR_EXIT_MESSAGE);
    }
}
