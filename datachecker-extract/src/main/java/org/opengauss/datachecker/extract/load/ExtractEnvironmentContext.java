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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

/**
 * ExtractEnvironmentContext
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/1
 * @since ：11
 */
@Component
public class ExtractEnvironmentContext {
    private static final Logger log = LogUtils.getLogger(ExtractEnvironmentContext.class);

    @Resource
    private MetaDataService metaDataService;
    @Value("${spring.check.core-pool-size}")
    private int maxCorePoolSize;
    @Value("${spring.check.max-retry-times}")
    private int maxRetryTimes;
    @Resource
    private ShutdownService shutdownService;
    @Resource
    private CheckingFeignClient checkingFeignClient;

    /**
     * Initialize the verification result environment
     */
    @Async
    public void loadDatabaseMetaData() {
        LogUtils.info(log, "extract database loader start");
        try {
            metaDataService.loadMetaDataOfSchemaCache();
            ThreadUtil.sleepHalfSecond();
            MetadataLoadProcess metadataLoadProcess = metaDataService.getMetadataLoadProcess();
            while (!metadataLoadProcess.isLoadSuccess()) {
                LogUtils.info(log, "extract service load  table meta ={}", metadataLoadProcess.getLoadCount());
                ThreadUtil.sleepOneSecond();
                metadataLoadProcess = metaDataService.getMetadataLoadProcess();
            }
            Endpoint endPoint = ConfigCache.getEndPoint();
            checkingFeignClient.refreshLoadMetadataCompleted(endPoint);
            if (metaDataService.mdsIsCheckTableEmpty(false)) {
                shutdownService.shutdown("load table metadata cache is empty!");
            }
            LogUtils.info(log, "extract service load table meta ={} , success", metadataLoadProcess.getLoadCount());
        } catch (ExtractException ex) {
            LogUtils.error(log, "{}extract database loader error", ErrorCode.CHECK_START_ERROR, ex);
            shutdownService.shutdown("extract database loader error!");
        }
    }
}
