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
import org.opengauss.datachecker.check.modules.task.TaskManagerService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.constraints.NotEmpty;
import java.util.Map;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "TaskStatusController", description = "Verification service - data extraction task status")
@Validated
@RestController
public class TaskStatusController {

    @Autowired
    private TaskManagerService taskManagerService;

    /**
     * Refresh the execution status of the data extraction table of the specified task
     *
     * @param tableName tableName
     * @param endpoint  endpoint {@link org.opengauss.datachecker.common.entry.enums.Endpoint}
     * @param status    status
     */
    @Operation(summary = "Refresh the execution status of the data extraction table of the specified task")
    @PostMapping("/table/extract/status")
    public void refreshTableExtractStatus(@NotEmpty String tableName, @NonNull Endpoint endpoint, int status) {
        taskManagerService.refreshTableExtractStatus(tableName, endpoint, status);
    }

    /**
     * query check status of all table
     *
     * @return status
     */
    @GetMapping("/query/all/table/status")
    public Map<String, Integer> queryTableCheckStatus() {
        return taskManagerService.queryTableCheckStatus();
    }
}
