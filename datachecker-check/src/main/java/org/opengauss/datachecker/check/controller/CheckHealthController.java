/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.controller;

import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * health check of the data check service
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@RestController
public class CheckHealthController {
    /**
     * health check of the data check service
     *
     * @return void
     */
    @GetMapping("/check/health")
    public Result<Void> health() {
        return Result.success();
    }
}
