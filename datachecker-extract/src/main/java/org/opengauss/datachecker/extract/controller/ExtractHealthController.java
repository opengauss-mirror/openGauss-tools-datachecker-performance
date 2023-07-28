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
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * health check of the data extraction service
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@RestController
public class ExtractHealthController {
    @Resource
    private DataAccessService dataAccessService;

    @Operation(summary = "data extraction health check")
    @GetMapping("/extract/health")
    public Result<Void> health() {
        dataAccessService.health();
        return Result.success();
    }
}
