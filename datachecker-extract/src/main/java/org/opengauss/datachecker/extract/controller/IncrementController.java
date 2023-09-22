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

package org.opengauss.datachecker.extract.controller;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.exception.NotSourceEndpointException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.debezium.DataConsolidationService;
import org.opengauss.datachecker.extract.debezium.IncrementDataAnalysisService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * IncrementController
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@RestController
public class IncrementController {
    private static final Logger log = LogUtils.getLogger();
    private static final AtomicBoolean IS_ENABLED_INCREMENT_SERVICE = new AtomicBoolean(false);

    @Resource
    private ExtractProperties extractProperties;
    @Resource
    private IncrementDataAnalysisService incrementDataAnalysisService;
    @Resource
    private DataConsolidationService dataConsolidationService;

    /**
     * start source increment monitor
     *
     * @return void
     */
    @PostMapping("/start/source/increment/monitor")
    Result<Void> startIncrementMonitor() {
        ConfigCache.setCheckMode(CheckMode.INCREMENT);
        if (!isSourceEndpoint()) {
            throw new NotSourceEndpointException();
        }
        if (IS_ENABLED_INCREMENT_SERVICE.get()) {
            log.info("the increment monitor service has started!");
        } else {
            log.info("start the increment monitor service,at {}", LocalDateTime.now());
            dataConsolidationService.initIncrementConfig();
            incrementDataAnalysisService.startIncrDataAnalysis();
            IS_ENABLED_INCREMENT_SERVICE.set(true);
        }
        return Result.success();
    }

    private boolean isSourceEndpoint() {
        return Objects.equals(Endpoint.SOURCE, extractProperties.getEndpoint());
    }

    /**
     * pause source increment monitor
     *
     * @return void
     */
    @PostMapping("/pause/resume/increment/monitor")
    Result<Void> pauseIncrementMonitor(@RequestParam("parseOrResume") boolean pauseOrResume) {
        if (!isSourceEndpoint()) {
            throw new NotSourceEndpointException();
        }
        dataConsolidationService.pauseOrResumeIncrementMonitor(pauseOrResume);
        return Result.success();
    }
}
