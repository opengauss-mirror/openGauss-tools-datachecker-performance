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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.annotation.Resource;
import java.util.List;

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

    /**
     * csv dispatcher tables
     *
     * @param list tables
     */
    @PostMapping("/csv/dispatcher/tables")
    public void dispatcherTables(@RequestBody List<String> list) {
        csvManagementService.dispatcherTables(list);
    }

    /**
     * fetch table count
     *
     * @return table count
     */
    @GetMapping("/fetch/csv/check/table/count")
    public Result<Integer> fetchCheckTableCount() {
        return Result.success(csvManagementService.fetchCheckTableCount());
    }
}
