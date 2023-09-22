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

import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.service.RepairStatement;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * data diff repair controller
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@RestController
public class DiffRepairController {
    @Resource
    private RepairStatement repairStatement;

    /**
     * DML statements required to generate a repair report
     *
     * @param repairEntry repairEntry
     * @return DML statement
     */
    @PostMapping("/extract/build/repair/statement/update")
    Result<List<String>> buildRepairStatementUpdateDml(@RequestBody RepairEntry repairEntry) {
        return Result.success(repairStatement.buildRepairStatementUpdateDml(repairEntry));
    }

    /**
     * DML statements required to generate a repair report
     *
     * @param repairEntry repairEntry
     * @return DML statement
     */
    @PostMapping("/extract/build/repair/statement/insert")
    Result<List<String>> buildRepairStatementInsertDml(@RequestBody RepairEntry repairEntry) {
        return Result.success(repairStatement.buildRepairStatementInsertDml(repairEntry));
    }

    /**
     * DML statements required to generate a repair report
     *
     * @param repairEntry repairEntry
     * @return DML statement
     */
    @PostMapping("/extract/build/repair/statement/delete")
    Result<List<String>> buildRepairStatementDeleteDml(@RequestBody RepairEntry repairEntry) {
        return Result.success(repairStatement.buildRepairStatementDeleteDml(repairEntry));
    }
}
