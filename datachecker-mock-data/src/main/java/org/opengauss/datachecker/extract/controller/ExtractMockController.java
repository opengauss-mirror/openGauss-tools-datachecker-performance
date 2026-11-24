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

package org.opengauss.datachecker.extract.controller;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.extract.service.ExtractMockDataService;
import org.opengauss.datachecker.extract.service.ExtractMockTableService;
import org.opengauss.datachecker.extract.vo.TableStatisticsInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * ExtractMockController
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@Slf4j
@RestController
public class ExtractMockController {
    @Autowired
    private ExtractMockDataService extractMockDataService;
    @Autowired
    private ExtractMockTableService extractMockTableService;

    /**
     * createTable
     *
     * @param tableName tableName
     * @return result
     * @throws Exception exception
     */
    @PostMapping("/mock/createTable")
    public String createTable(@RequestParam("tableName") String tableName) throws Exception {
        return extractMockTableService.createTable(tableName);
    }

    /**
     * queryTableStatisticsInfo
     *
     * @return TableStatisticsInfo
     */
    @GetMapping("/mock/statistics/table/info")
    public List<TableStatisticsInfo> queryTableStatisticsInfo() {
        return extractMockTableService.getAllTableInfo();
    }

    /**
     * batchMockData
     *
     * @param tableName         tableName
     * @param totalCount        totalCount
     * @param threadCount       threadCount
     * @param shouldCreateTable shouldCreateTable
     * @return result
     */
    @PostMapping("/batch/mock/data")
    public String batchMockData(@RequestParam("tableName") String tableName,
        @RequestParam("totalCount") long totalCount, @RequestParam("threadCount") int threadCount,
        @RequestParam("shouldCreateTable") boolean shouldCreateTable) {
        try {
            if (shouldCreateTable) {
                extractMockTableService.createTable(tableName);
            }
            extractMockDataService.batchMockData(tableName, totalCount, threadCount);
        } catch (Exception throwables) {
            log.error(throwables.getMessage());
        }
        return "OK";
    }
}
