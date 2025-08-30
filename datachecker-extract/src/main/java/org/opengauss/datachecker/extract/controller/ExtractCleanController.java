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

import cn.hutool.core.thread.ThreadUtil;
import io.swagger.v3.oas.annotations.Operation;

import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.kafka.KafkaManagerService;
import org.opengauss.datachecker.extract.service.CsvManagementService;
import org.opengauss.datachecker.extract.service.DataExtractService;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;

/**
 * Clearing the environment at the extraction endpoint
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@RestController
public class ExtractCleanController {
    @Resource
    private ApplicationContext applicationContext;
    @Resource
    private DataExtractService dataExtractService;
    @Resource
    private KafkaManagerService kafkaManagerService;
    @Resource
    private ShutdownService shutdownService;
    @Resource
    private CsvManagementService csvManagementService;
    @Resource
    private SliceProcessorContext sliceProcessorContext;

    /**
     * clear the endpoint information and reinitialize the environment.
     *
     * @return interface invoking result
     */
    @Operation(summary = "clear the endpoint information and reinitialize the environment")
    @PostMapping("/extract/clean/environment")
    Result<Void> cleanEnvironment(@RequestParam(name = "processNo") String processNo) {
        dataExtractService.cleanBuildTask();
        kafkaManagerService.cleanKafka(processNo);
        return Result.success();
    }

    @Operation(summary = "clears the task cache information of the current ednpoint")
    @PostMapping("/extract/clean/task")
    Result<Void> cleanTask() {
        dataExtractService.cleanBuildTask();
        return Result.success();
    }

    /**
     * Shutdown the extractor
     *
     * @param message shutdown message
     * @return interface invoking result
     */
    @PostMapping("/extract/shutdown")
    Result<Void> shutdown(@RequestBody String message) {
        ThreadUtil.newThread(() -> {
            csvManagementService.close();
            sliceProcessorContext.shutdownSliceStatusFeedbackService();
            ThreadUtil.sleep(500);
            shutdownService.shutdown(message);
            if (applicationContext instanceof ConfigurableApplicationContext configurableApplicationContext) {
                configurableApplicationContext.close();
            }
        }, "extract-shutdown-thread").start();
        return Result.success();
    }
}
