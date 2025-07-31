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
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.check.service.IncrementManagerService;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.constraints.NotEmpty;
import java.util.List;

/**
 * IncrementManagerController
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "IncrementManagerController", description = "Verification service - incremental verification management")
@Validated
@RestController
public class IncrementManagerController {

    @Autowired
    private IncrementManagerService incrementManagerService;

    /**
     * Incremental verification log notification
     *
     * @param dataLogList Incremental verification log
     */
    @Operation(summary = "Incremental verification log notification")
    @PostMapping("/notify/source/increment/data/logs")
    public void notifySourceIncrementDataLogs(@RequestBody @NotEmpty List<SourceDataLog> dataLogList) {
        incrementManagerService.notifySourceIncrementDataLogs(dataLogList);
    }

}
