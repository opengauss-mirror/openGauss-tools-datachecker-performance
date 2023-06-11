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
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * ExtractDatabaseLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Slf4j
@Order(97)
@Service
public class ExtractDatabaseLoader extends AbstractExtractLoader {
    private MetadataLoadProcess metadataLoadProcess;
    @Resource
    private MetaDataService metaDataService;

    /**
     * Initialize the verification result environment
     */
    @Override
    public void load(ExtractEnvironment extractEnvironment) {
        log.info("extract database loader start");
        metaDataService.loadMetaDataOfSchemaCache();
        ThreadUtil.sleepHalfSecond();
        metadataLoadProcess = metaDataService.getMetadataLoadProcess();
        while (!metadataLoadProcess.isLoadSuccess()) {
            log.info("extract service load  table meta ={}", metadataLoadProcess.getLoadCount());
            ThreadUtil.sleepOneSecond();
            metadataLoadProcess = metaDataService.getMetadataLoadProcess();
        }
        extractEnvironment.setLoadSuccess(metadataLoadProcess.isLoadSuccess());
        log.info("extract service load table meta ={} , success", metadataLoadProcess.getLoadCount());
    }
}
