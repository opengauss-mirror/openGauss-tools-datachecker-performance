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
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * ExtractEnvironmentContext
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/1
 * @since ：11
 */
@Component
public class ExtractEnvironmentContext {
    private static final Logger log = LogUtils.getLogger();
    @Resource
    private MetaDataService metaDataService;
    @Value("${spring.check.core-pool-size}")
    private int maxCorePoolSize;
    @Value("${spring.check.max-retry-times}")
    private int maxRetryTimes;
    @Resource
    private ShutdownService shutdownService;

    /**
     * Initialize the verification result environment
     */
    @Async
    public void loadProgressChecking() {
        int retryTime = 0;
        if (!metaDataService.isCheckTableEmpty(true)) {
            while (metaDataService.queryMetaDataOfSchemaCache().isEmpty()) {
                ThreadUtil.sleepHalfSecond();
                retryTime++;
                if (retryTime > maxRetryTimes) {
                    shutdownService.shutdown("load table metadata cache is empty!");
                }
            }
        }
    }

    /**
     * Initialize the verification result environment
     */
    @Async
    public void loadDatabaseMetaData() {
        log.info("extract database loader start");
        metaDataService.loadMetaDataOfSchemaCache();
        ThreadUtil.sleepHalfSecond();
        MetadataLoadProcess metadataLoadProcess = metaDataService.getMetadataLoadProcess();
        while (!metadataLoadProcess.isLoadSuccess()) {
            log.info("extract service load  table meta ={}", metadataLoadProcess.getLoadCount());
            ThreadUtil.sleepOneSecond();
            metadataLoadProcess = metaDataService.getMetadataLoadProcess();
        }
        log.info("extract service load table meta ={} , success", metadataLoadProcess.getLoadCount());
    }
}
