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

package org.opengauss.datachecker.check.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "CheckStartController", description = "Verification service - verification service start command")
@Validated
@RestController
@RequestMapping
public class CheckStartController {
    @Resource
    private CheckService checkService;
    @Resource
    private EndpointMetaDataManager endpointMetaDataManager;

    /**
     * Turn on verification
     *
     * @param checkMode checkMode {@value CheckMode#API_DESCRIPTION}
     * @return verification process info
     */
    @Operation(summary = "Turn on verification")
    @PostMapping("/start/check")
    public Result<String> statCheck(
        @Parameter(name = "checkMode", description = CheckMode.API_DESCRIPTION) @RequestParam("checkMode")
            CheckMode checkMode) {
        return Result.success(checkService.start(checkMode));
    }

    /**
     * <pre>
     * Stop the verification service and clean up the verification service.
     * Comprehensively clean up the verification status in the current process
     * and the extracted data and other relevant information"
     * </pre>
     *
     * @return request result
     */
    @PostMapping("/stop/clean/check")
    public Result<Void> cleanCheck() {
        checkService.cleanCheck();
        return Result.success();
    }

    /**
     * Query the current verification service process number
     *
     * @return process number
     */
    @Operation(summary = "Query the current verification service process number")
    @GetMapping("/get/check/process")
    public Result<String> getCurrentCheckProcess() {
        return Result.success(checkService.getCurrentCheckProcess());
    }

    /**
     * refreshLoadMetadataStatus
     *
     * @param endpoint endpoint
     */
    @PostMapping("/load/metadata/completed")
    public void refreshLoadMetadataCompleted(@RequestParam("endpoint") Endpoint endpoint) {
        endpointMetaDataManager.refreshLoadMetadataStatus(endpoint);
    }
}
