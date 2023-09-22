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

import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.service.BaseManagementService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * data base service
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Tag(name = "data base service")
@RestController
public class ExtractBaseController {
    @Resource
    private BaseManagementService baseManagementService;

    /**
     * fetch table count
     *
     * @return table count
     */
    @GetMapping("/fetch/check/table/count")
    public Result<Integer> fetchCheckTableCount() {
        return Result.success(baseManagementService.fetchCheckTableCount());
    }
}
