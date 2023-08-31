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

import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.service.CsvManagementService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * CheckCsvController
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@RestController
@RequestMapping
public class CheckCsvController {
    @Resource
    private CsvManagementService csvManagementService;

    /**
     * Turn on verification
     *
     * @return verification process info
     */
    @PostMapping("/start/csv/service")
    public Result<Void> statCsvExtractService() {
        Boolean isSync = ConfigCache.getBooleanValue(ConfigConstants.CSV_SYNC);
        if (isSync) {
            csvManagementService.startCsvProcess();
        } else {
            csvManagementService.startCsvNoSliceLogProcess();
        }
        return Result.success();
    }
}
