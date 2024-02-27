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

import org.opengauss.datachecker.check.service.CsvProcessManagement;
import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@RestController
@RequestMapping
public class CheckCsvController {
    @Autowired
    private CsvProcessManagement csvProcessManagement;
    @Resource
    private TaskRegisterCenter registerCenter;

    /**
     * csv task dispatcher
     *
     * @param completedTableList completedTableList
     */
    @PostMapping("/notify/check/table/index/completed")
    public void notifyTableIndexCompleted(@RequestBody List<String> completedTableList) {
        csvProcessManagement.taskDispatcher(completedTableList);
    }

    /**
     * notifyCheckIgnoreTable
     *
     * @param endpoint endpoint
     * @param table    table
     * @param reason   reason
     */
    @PostMapping("/notify/check/csv/ignore")
    void notifyCheckIgnoreTable(@RequestParam("endpoint") Endpoint endpoint, @RequestParam("table") String table,
        @RequestParam("reason") String reason) {
        registerCenter.addCheckIgnoreTable(endpoint, table, reason);
    }
}
