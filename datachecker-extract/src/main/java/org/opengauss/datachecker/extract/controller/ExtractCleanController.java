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

import io.swagger.v3.oas.annotations.Operation;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.kafka.KafkaManagerService;
import org.opengauss.datachecker.extract.service.DataExtractService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

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
    private DataExtractService dataExtractService;
    @Resource
    private KafkaManagerService kafkaManagerService;
    @Resource
    private ShutdownService shutdownService;

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

    @PostMapping("/extract/shutdown")
    Result<Void> shutdown(@RequestBody String message) {
        shutdownService.shutdown(message);
        return Result.success();
    }
}
