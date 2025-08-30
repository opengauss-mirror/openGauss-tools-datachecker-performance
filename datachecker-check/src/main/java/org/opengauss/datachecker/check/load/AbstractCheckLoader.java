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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.event.CustomEventHistory;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.LogUtils;

import cn.hutool.core.thread.ThreadUtil;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import jakarta.annotation.Resource;

/**
 * AbstractCheckLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
public abstract class AbstractCheckLoader implements CheckLoader {
    protected static final Logger log = LogUtils.getLogger(CheckLoader.class);

    @Value("${data.check.max-retry-times}")
    protected int maxRetryTimes;
    @Value("${data.check.retry-interval-times}")
    protected int retryIntervalTimes;

    @Resource
    private ShutdownService shutdownService;
    @Resource
    private FeignClientService feignClient;
    @Resource
    private CustomEventHistory customEventHistory;
    @Resource
    private ApplicationContext applicationContext;

    /**
     * shutdown app
     *
     * @param message shutdown message
     */
    public void shutdown(String message) {
        while (!customEventHistory.checkAllEventCompleted()) {
            ThreadUtil.sleep(1000);
        }
        ThreadUtil.newThread(() -> {
            feignClient.shutdown(message);
            ThreadUtil.sleep(1000);
            shutdownService.shutdown(message);
            if (applicationContext instanceof ConfigurableApplicationContext configurableApplicationContext) {
                configurableApplicationContext.close();
            }
        }, "shutdown-check").start();
    }
}
