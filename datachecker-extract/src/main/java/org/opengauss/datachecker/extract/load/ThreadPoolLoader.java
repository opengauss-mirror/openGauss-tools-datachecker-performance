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

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;

/**
 * ThreadPoolLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Slf4j
@Order(101)
@Service
public class ThreadPoolLoader extends AbstractExtractLoader {
    @Resource
    private MetaDataService metaDataService;
    @Value("${spring.check.core-pool-size}")
    private int maxCorePoolSize;
    @Value("${spring.check.max-retry-times}")
    private int maxRetryTimes;

    /**
     * Initialize the verification result environment
     *
     * @param extractEnvironment extractEnvironment
     */
    @Override
    public void load(ExtractEnvironment extractEnvironment) {
        int retryTime = 0;
        if (!metaDataService.isCheckTableEmpty(true)) {
            while (metaDataService.queryMetaDataOfSchemaCache().isEmpty()) {
                ThreadUtil.sleepHalfSecond();
                retryTime++;
                if (retryTime > maxRetryTimes) {
                    shutdown("load table metadata cache is empty!");
                }
            }
        }
        final Map<String, TableMetadata> metadataMap = metaDataService.queryMetaDataOfSchemaCache();
        final int queueSize = metadataMap.size();
        extractEnvironment.setMaxCorePoolSize(maxCorePoolSize);
        extractEnvironment.setQueueSize(queueSize);
    }
}
